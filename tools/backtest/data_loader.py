"""
data_loader.py — ClosingBell 종합 백테스트 데이터 로더
=====================================================
- 정제된 유니버스(코스피+코스닥) 기반 OHLCV 로딩
- 우선주 태깅 (포함하되 분리 분석 가능)
- 메모리 효율적 로딩 + 멀티프로세싱 지원
- 일자별 유니버스 구성 (거래량/거래대금 순위 등)

사용법:
    from data_loader import OHLCVLoader

    loader = OHLCVLoader(
        ohlcv_dir="C:/Coding/data/ohlcv",
        whitelist_csv="universe_whitelist.csv"
    )

    # 전종목 로드 (딕셔너리)
    all_data = loader.load_all()

    # 보통주만
    common_data = loader.load_all(exclude_preferred=True)

    # 특정 날짜의 거래량 TOP N
    top5 = loader.get_daily_top(date="2025-06-15", by="volume", n=5)
"""

import os
import re
import sys
import json
import gzip
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp


# ══════════════════════════════════════════════
# 종목 분류 유틸
# ══════════════════════════════════════════════

def classify_stock(code: str, name: str) -> dict:
    """
    종목코드+종목명으로 유형 분류.
    Returns: {
        'is_preferred': bool,   # 우선주
        'is_etf': bool,         # ETF
        'is_etn': bool,         # ETN
        'is_spac': bool,        # 스팩
        'is_reit': bool,        # 리츠
        'is_common': bool,      # 순수 보통주
        'stock_type': str       # 'common'|'preferred'|'etf'|'etn'|'spac'|'reit'|'other'
    }
    """
    code = str(code).zfill(6)
    name = str(name) if name else ""

    # 우선주: 종목명 끝이 "우", "우B", "우C" 또는 코드 끝자리 5,7,9 + 이름에 "우"
    is_pref_name = bool(re.search(r'우[BbCc]?$|\(우\)$', name))
    is_pref_code = code[-1] in ('5', '7', '9') and '우' in name
    is_preferred = is_pref_name or is_pref_code

    # ETF
    etf_kw = ['KODEX', 'TIGER', 'KBSTAR', 'ARIRANG', 'SOL', 'HANARO',
              'KOSEF', 'ACE', 'PLUS', 'TIMEFOLIO', 'WOORI', 'BNK',
              'VITA', 'FOCUS', 'KTOP', 'TREX', 'DAISHIN', 'KoAct',
              'TRUE', 'RISE', '인버스', '레버리지', '2X', '곱버스']
    is_etf = any(kw in name for kw in etf_kw)

    # ETN
    is_etn = 'ETN' in name.upper()

    # 스팩
    is_spac = '스팩' in name or 'SPAC' in name.upper()

    # 리츠
    is_reit = '리츠' in name or '인프라' in name

    is_common = not any([is_preferred, is_etf, is_etn, is_spac, is_reit])

    if is_common:
        stype = 'common'
    elif is_preferred:
        stype = 'preferred'
    elif is_etf:
        stype = 'etf'
    elif is_etn:
        stype = 'etn'
    elif is_spac:
        stype = 'spac'
    elif is_reit:
        stype = 'reit'
    else:
        stype = 'other'

    return {
        'is_preferred': is_preferred,
        'is_etf': is_etf,
        'is_etn': is_etn,
        'is_spac': is_spac,
        'is_reit': is_reit,
        'is_common': is_common,
        'stock_type': stype,
    }


# ══════════════════════════════════════════════
# 단일 종목 로드 함수 (멀티프로세싱용 — 모듈 레벨)
# ══════════════════════════════════════════════

def _load_single_csv(args):
    """프로세스 풀에서 호출되는 단일 CSV 로더"""
    filepath, code, name, market = args
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')

        # 컬럼명 정리 (공백/BOM 제거 + 소문자)
        df.columns = df.columns.str.strip().str.lower()

        # 필수 컬럼 확인
        required = {'date', 'open', 'high', 'low', 'close', 'volume'}
        if not required.issubset(set(df.columns)):
            return None

        df['date'] = pd.to_datetime(df['date'])

        df = df.sort_values('date').reset_index(drop=True)

        # 메타 정보 추가
        df['code'] = code
        df['name'] = name
        df['market'] = market

        # 분류 정보
        cls = classify_stock(code, name)
        df['stock_type'] = cls['stock_type']
        df['is_preferred'] = cls['is_preferred']
        df['is_common'] = cls['is_common']

        # 기본 파생 컬럼
        df['trade_value'] = df['close'] * df['volume']  # 거래대금 (근사)
        df['change_rate'] = df['close'].pct_change() * 100  # 등락률(%)

        return (code, df)
    except Exception as e:
        return None


# ══════════════════════════════════════════════
# 메인 로더 클래스
# ══════════════════════════════════════════════

class OHLCVLoader:
    """
    OHLCV 데이터 로더.

    특징:
    - 화이트리스트 CSV 기반 코넥스 제외
    - 우선주 태깅 (포함하되 분리 가능)
    - 멀티프로세싱 병렬 로딩
    - ETF/스팩/리츠 제외 옵션
    """

    def __init__(self,
                 ohlcv_dir: str,
                 whitelist_csv: str = "universe_whitelist.csv",
                 mapping_csv: str = None,
                 n_workers: int = None):
        """
        Args:
            ohlcv_dir: OHLCV CSV 폴더 경로
            whitelist_csv: generate_universe.py가 만든 CSV (None이면 필터 안 함)
            mapping_csv: stock_mapping.csv (종목명 매핑용, 없으면 파일명 기반)
            n_workers: 병렬 워커 수 (None=CPU 코어수)
        """
        self.ohlcv_dir = Path(ohlcv_dir)
        self.n_workers = n_workers or max(1, mp.cpu_count() - 1)

        # 화이트리스트 로드
        self.whitelist = {}  # {code: {'name': ..., 'market': ...}}
        if whitelist_csv and os.path.exists(whitelist_csv):
            wl = pd.read_csv(whitelist_csv, dtype={'code': str}, encoding='utf-8-sig')
            wl['code'] = wl['code'].str.zfill(6)
            for _, row in wl.iterrows():
                self.whitelist[row['code']] = {
                    'name': row.get('name', ''),
                    'market': row.get('market', ''),
                    'stock_type': row.get('stock_type', 'common'),
                    'is_preferred': int(row.get('is_preferred', 0)),
                }
            print(f"📋 화이트리스트: {len(self.whitelist)}종목")
        else:
            print("⚠️ 화이트리스트 없음 — 전종목 로드 (코넥스 포함 주의)")

        # 매핑 CSV (종목명 보강용)
        self.mapping = {}
        if mapping_csv and os.path.exists(mapping_csv):
            mp_df = pd.read_csv(mapping_csv, dtype={'code': str})
            if 'code' in mp_df.columns and 'name' in mp_df.columns:
                for _, row in mp_df.iterrows():
                    self.mapping[str(row['code']).zfill(6)] = row['name']

    def _get_file_list(self) -> List[Tuple[str, str, str, str]]:
        """로드할 파일 목록 생성: [(filepath, code, name, market), ...]"""
        files = []
        for f in sorted(self.ohlcv_dir.glob("*.csv")):
            code = f.stem.zfill(6)

            # 화이트리스트 필터
            if self.whitelist:
                if code not in self.whitelist:
                    continue
                name = self.whitelist[code].get('name', '')
                market = self.whitelist[code].get('market', '')
            else:
                name = self.mapping.get(code, '')
                market = ''

            files.append((str(f), code, name, market))

        return files

    def load_all(self,
                 exclude_preferred: bool = False,
                 exclude_etf: bool = True,
                 exclude_spac: bool = True,
                 exclude_reit: bool = True,
                 min_rows: int = 60) -> Dict[str, pd.DataFrame]:
        """
        전종목 OHLCV 로드.

        Args:
            exclude_preferred: True면 우선주 제외
            exclude_etf: True면 ETF/ETN 제외 (기본 True)
            exclude_spac: True면 스팩 제외 (기본 True)
            exclude_reit: True면 리츠 제외 (기본 True)
            min_rows: 최소 데이터 일수 (너무 적으면 지표 계산 불가)

        Returns:
            {code: DataFrame} 딕셔너리
        """
        file_list = self._get_file_list()
        print(f"📂 로드 대상: {len(file_list)}종목, 워커: {self.n_workers}개")

        data = {}
        loaded = 0
        skipped_type = 0
        skipped_rows = 0

        # 멀티프로세싱 로딩
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            futures = {executor.submit(_load_single_csv, args): args
                       for args in file_list}

            for future in as_completed(futures):
                result = future.result()
                if result is None:
                    continue

                code, df = result

                # 최소 데이터 체크
                if len(df) < min_rows:
                    skipped_rows += 1
                    continue

                # 유형별 필터
                stype = df['stock_type'].iloc[0]
                if exclude_preferred and stype == 'preferred':
                    skipped_type += 1
                    continue
                if exclude_etf and stype in ('etf', 'etn'):
                    skipped_type += 1
                    continue
                if exclude_spac and stype == 'spac':
                    skipped_type += 1
                    continue
                if exclude_reit and stype == 'reit':
                    skipped_type += 1
                    continue

                data[code] = df
                loaded += 1

                if loaded % 500 == 0:
                    print(f"  ... {loaded}종목 로드 완료")

        print(f"✅ 로드 완료: {loaded}종목")
        if skipped_type > 0:
            print(f"  유형 필터: {skipped_type}종목 제외")
        if skipped_rows > 0:
            print(f"  데이터 부족: {skipped_rows}종목 제외")

        return data

    def load_all_merged(self, **kwargs) -> pd.DataFrame:
        """전종목을 하나의 DataFrame으로 병합 (메모리 주의)"""
        data = self.load_all(**kwargs)
        if not data:
            return pd.DataFrame()
        return pd.concat(data.values(), ignore_index=True)

    def get_trading_dates(self, data: Dict[str, pd.DataFrame]) -> List[str]:
        """전체 거래일 목록 추출"""
        all_dates = set()
        for df in data.values():
            all_dates.update(df['date'].dt.strftime('%Y-%m-%d').tolist())
        return sorted(all_dates)

    def get_daily_snapshot(self,
                           data: Dict[str, pd.DataFrame],
                           date: str) -> pd.DataFrame:
        """
        특정 날짜의 전종목 스냅샷.
        거래량 TOP, 거래대금 TOP 등 추출에 사용.
        """
        rows = []
        for code, df in data.items():
            day = df[df['date'] == date]
            if len(day) == 0:
                continue
            rows.append(day.iloc[0].to_dict())

        if not rows:
            return pd.DataFrame()

        snap = pd.DataFrame(rows)
        snap = snap.sort_values('trade_value', ascending=False)
        snap['volume_rank'] = range(1, len(snap) + 1)

        # 거래대금 순위도
        snap = snap.sort_values('trade_value', ascending=False)
        snap['value_rank'] = range(1, len(snap) + 1)

        return snap


# ══════════════════════════════════════════════
# 미래 수익률 계산 유틸
# ══════════════════════════════════════════════

def calc_forward_returns(df: pd.DataFrame,
                          signal_idx: int,
                          hold_days: tuple = (1,2,3,5,7,10,15,20)) -> dict:
    """
    시그널 발생일(signal_idx) 이후 D+N 수익률 계산.

    Args:
        df: 단일 종목 DataFrame (date 정렬됨)
        signal_idx: 시그널 발생 행 인덱스
        hold_days: 측정할 보유일수 튜플

    Returns:
        {
            'D+1_return': float,  # 종가 기준 수익률(%)
            'D+1_win': bool,
            'D+1_high_2pct': bool,  # 장중 고가 2%+ 도달 여부
            'D+3_return': float,
            ...
        }
    """
    result = {}
    entry_close = df.iloc[signal_idx]['close']

    if entry_close <= 0:
        return {f'D+{d}_return': np.nan for d in hold_days}

    for d in hold_days:
        target_idx = signal_idx + d
        if target_idx >= len(df):
            result[f'D+{d}_return'] = np.nan
            result[f'D+{d}_win'] = np.nan
            result[f'D+{d}_high_2pct'] = np.nan
            continue

        future_close = df.iloc[target_idx]['close']
        ret = (future_close / entry_close - 1) * 100
        result[f'D+{d}_return'] = round(ret, 4)
        result[f'D+{d}_win'] = 1 if ret > 0 else 0

        # 장중 고가 2%+ 도달 여부 (D+1 ~ D+d 기간 중)
        period = df.iloc[signal_idx+1:target_idx+1]
        if len(period) > 0:
            max_high = period['high'].max()
            high_ret = (max_high / entry_close - 1) * 100
            result[f'D+{d}_high_2pct'] = 1 if high_ret >= 2.0 else 0
        else:
            result[f'D+{d}_high_2pct'] = np.nan

    return result


# ══════════════════════════════════════════════
# 결과 집계 유틸
# ══════════════════════════════════════════════

def summarize_results(results_df: pd.DataFrame,
                       hold_days: tuple = (1,2,3,5,7,10,15,20),
                       group_by: str = None) -> pd.DataFrame:
    """
    백테스트 결과 요약.

    Args:
        results_df: 시그널별 결과 DataFrame
        hold_days: 요약할 보유일수
        group_by: 그룹별 비교 (예: 'stock_type', 'year', 'rank')

    Returns:
        요약 DataFrame
    """
    rows = []

    groups = [(None, results_df)]
    if group_by and group_by in results_df.columns:
        groups = list(results_df.groupby(group_by))

    for gname, gdf in groups:
        row = {'group': gname if gname else 'ALL', 'n_signals': len(gdf)}

        for d in hold_days:
            ret_col = f'D+{d}_return'
            win_col = f'D+{d}_win'

            if ret_col in gdf.columns:
                valid = gdf[ret_col].dropna()
                row[f'D+{d}_mean'] = round(valid.mean(), 4) if len(valid) > 0 else np.nan
                row[f'D+{d}_median'] = round(valid.median(), 4) if len(valid) > 0 else np.nan
                row[f'D+{d}_std'] = round(valid.std(), 4) if len(valid) > 0 else np.nan

            if win_col in gdf.columns:
                valid = gdf[win_col].dropna()
                row[f'D+{d}_winrate'] = round(valid.mean() * 100, 2) if len(valid) > 0 else np.nan

        rows.append(row)

    return pd.DataFrame(rows)


def summarize_by_year(results_df: pd.DataFrame,
                       date_col: str = 'date') -> pd.DataFrame:
    """연도별 성과 요약"""
    if date_col not in results_df.columns:
        return pd.DataFrame()

    results_df = results_df.copy()
    results_df['year'] = pd.to_datetime(results_df[date_col]).dt.year
    return summarize_results(results_df, group_by='year')


if __name__ == "__main__":
    # 테스트
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--ohlcv', default='C:/Coding/data/ohlcv')
    parser.add_argument('--whitelist', default='universe_whitelist.csv')
    parser.add_argument('--workers', type=int, default=None)
    args = parser.parse_args()

    loader = OHLCVLoader(args.ohlcv, args.whitelist, n_workers=args.workers)
    data = loader.load_all(exclude_preferred=False)  # 우선주 포함

    # 통계
    common = {k: v for k, v in data.items() if v['is_common'].iloc[0]}
    preferred = {k: v for k, v in data.items() if v['is_preferred'].iloc[0]}

    print(f"\n📊 유니버스 구성:")
    print(f"  보통주: {len(common)}종목")
    print(f"  우선주: {len(preferred)}종목")
    print(f"  합계:   {len(data)}종목")

    # 거래일 수
    dates = loader.get_trading_dates(data)
    print(f"  거래일: {len(dates)}일 ({dates[0]} ~ {dates[-1]})")