"""
master_backtest.py — ClosingBell 종합 백테스트 오케스트레이터
============================================================
12개 시뮬 모듈을 병렬로 실행하고 결과를 통합.

사용법:
  python master_backtest.py --all                    # 전체 실행
  python master_backtest.py --module D F E           # 특정 모듈만
  python master_backtest.py --all --workers 8        # 워커 수 지정
  python master_backtest.py --all --start 2020-01-01 # 기간 제한
  python master_backtest.py --report                 # 기존 결과로 리포트만

실행 시간: 전체 약 3~8시간 (CPU/데이터 양에 따라)
"""

import os
import sys
import time
import json
import argparse
import pandas as pd
import numpy as np
import multiprocessing as mp
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

# 프로젝트 루트
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))

from data_loader import OHLCVLoader, calc_forward_returns, summarize_results, summarize_by_year

# ══════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════

DEFAULT_CONFIG = {
    'ohlcv_dir': 'C:/Coding/data/ohlcv',
    'whitelist_csv': str(SCRIPT_DIR / 'universe_whitelist.csv'),
    'mapping_csv': 'C:/Coding/data/stock_mapping.csv',
    'results_dir': str(SCRIPT_DIR / 'results'),
    'hold_days': (1, 2, 3, 5, 7, 10, 15, 20),
    'min_trade_value': 0,         # 최소 거래대금 (0=필터 없음)
    'min_price': 0,               # 최소 주가
    'start_date': None,           # None=전체기간
    'end_date': None,
}

# 모듈 레지스트리
MODULE_REGISTRY = {
    'A': {'name': '거래량 TOP5 기본', 'func': 'run_mod_a'},
    'B': {'name': '거래량 TOP5 필터', 'func': 'run_mod_b'},
    'C': {'name': '거래량 TOP5 보조지표', 'func': 'run_mod_c'},
    'D': {'name': '점수제 종합 (v3.8)', 'func': 'run_mod_d'},
    'E': {'name': '순위별 보유기간', 'func': 'run_mod_e'},
    'F': {'name': '눌림목 진입', 'func': 'run_mod_f'},
    'G': {'name': 'K값 변동성 돌파', 'func': 'run_mod_g'},
    'H': {'name': 'RSI 반전 (Cameron)', 'func': 'run_mod_h'},
    'I': {'name': '유목민 거감음봉', 'func': 'run_mod_i'},
    'J': {'name': '파동매매 OBV', 'func': 'run_mod_j'},
    'K': {'name': '시황/캘린더', 'func': 'run_mod_k'},
    'L': {'name': '150억봉 추격', 'func': 'run_mod_l'},
}


# ══════════════════════════════════════════════
# 공통 지표 계산 (종목별 1회)
# ══════════════════════════════════════════════

def precompute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    한 종목의 전체 기간에 대해 공통 지표를 미리 계산.
    각 모듈이 필요한 지표를 여기서 한 번에 계산해서 중복을 방지.
    """
    df = df.copy()
    c = df['close']
    h = df['high']
    l = df['low']
    o = df['open']
    v = df['volume']

    # ── 이동평균 ──
    df['ma5'] = c.rolling(5).mean()
    df['ma20'] = c.rolling(20).mean()
    df['ma60'] = c.rolling(60).mean()
    df['ma120'] = c.rolling(120).mean()

    # ── MA 기울기 ──
    df['ma20_slope'] = (df['ma20'] - df['ma20'].shift(3)) / df['ma20'].shift(3) * 100

    # ── 이격도 ──
    df['disparity5'] = (c / df['ma5'] - 1) * 100
    df['disparity20'] = (c / df['ma20'] - 1) * 100

    # ── CCI(22) ──
    tp = (h + l + c) / 3
    ma_tp = tp.rolling(22).mean()
    md = tp.rolling(22).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
    df['cci'] = (tp - ma_tp) / (0.015 * md.replace(0, np.nan))

    # ── RSI(14) ──
    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    df['rsi'] = 100 - (100 / (1 + gain / loss.replace(0, np.nan)))

    # ── 볼린저밴드 ──
    bb_mid = c.rolling(20).mean()
    bb_std = c.rolling(20).std()
    df['bb_upper'] = bb_mid + 2 * bb_std
    df['bb_lower'] = bb_mid - 2 * bb_std
    df['bb_pctb'] = (c - df['bb_lower']) / (df['bb_upper'] - df['bb_lower']).replace(0, np.nan)

    # ── 거래량 지표 ──
    df['vol_ma5'] = v.rolling(5).mean()
    df['vol_ma20'] = v.rolling(20).mean()
    df['vol_ratio5'] = v / df['vol_ma5'].replace(0, np.nan)
    df['vol_ratio20'] = v / df['vol_ma20'].replace(0, np.nan)

    # ── OBV ──
    obv = [0.0]
    for i in range(1, len(df)):
        if c.iloc[i] > c.iloc[i-1]:
            obv.append(obv[-1] + v.iloc[i])
        elif c.iloc[i] < c.iloc[i-1]:
            obv.append(obv[-1] - v.iloc[i])
        else:
            obv.append(obv[-1])
    df['obv'] = obv
    df['obv_ma20'] = pd.Series(obv, index=df.index).rolling(20).mean()

    # ── 캔들 패턴 ──
    body = c - o
    full_range = (h - l).replace(0, np.nan)
    df['candle_body_pct'] = body / full_range  # -1~1
    df['upper_shadow'] = (h - np.maximum(c, o)) / full_range
    df['lower_shadow'] = (np.minimum(c, o) - l) / full_range
    df['is_bullish'] = (c > o).astype(int)

    # ── 연속 양봉 ──
    consec = []
    cnt = 0
    for i in range(len(df)):
        if df['is_bullish'].iloc[i]:
            cnt += 1
        else:
            cnt = 0
        consec.append(cnt)
    df['consec_bullish'] = consec

    # ── 등락률 ──
    df['change_rate'] = c.pct_change() * 100

    # ── 거래대금 ──
    df['trade_value'] = c * v

    # ── K값 돌파가 ──
    df['prev_range'] = (h.shift(1) - l.shift(1))
    df['breakout_price'] = o + df['prev_range'] * 0.3

    return df


# ══════════════════════════════════════════════
# 종목별 병렬 처리 워커
# ══════════════════════════════════════════════

def _process_stock(args):
    """
    단일 종목에 대해 모든 활성 모듈의 시그널을 감지하고 수익률 계산.
    멀티프로세싱 풀에서 호출됨.

    Returns: list of signal dicts
    """
    code, df_raw, active_modules, config = args
    hold_days = config.get('hold_days', (1,2,3,5,7,10,15,20))
    start_date = config.get('start_date')
    end_date = config.get('end_date')

    try:
        # 지표 계산
        df = precompute_indicators(df_raw)

        # 기간 필터
        if start_date:
            df = df[df['date'] >= start_date]
        if end_date:
            df = df[df['date'] <= end_date]

        if len(df) < 60:
            return []

        df = df.reset_index(drop=True)
        signals = []

        # ── 각 날짜를 순회하며 시그널 감지 ──
        for i in range(60, len(df) - 20):  # 앞 60일 지표 안정화, 뒤 20일 수익률 계산용
            row = df.iloc[i]
            date_str = row['date'].strftime('%Y-%m-%d') if hasattr(row['date'], 'strftime') else str(row['date'])[:10]

            base_info = {
                'code': code,
                'name': row.get('name', ''),
                'date': date_str,
                'year': int(date_str[:4]),
                'close': row['close'],
                'stock_type': row.get('stock_type', ''),
                'is_preferred': row.get('is_preferred', False),
                'market': row.get('market', ''),
            }

            # 미래 수익률 (공통)
            fwd = calc_forward_returns(df, i, hold_days)

            # ── 모듈별 시그널 감지 ──

            # A: 거래량 TOP5 → 일자별 순위가 필요하므로 여기선 스킵
            #    (master에서 일자별로 별도 처리)

            # D: 점수제 — 현행 9지표 계산
            if 'D' in active_modules:
                score = _calc_score_v38(df, i)
                if score is not None and score['total'] >= 50:
                    sig = {**base_info, **fwd, 'module': 'D',
                           'score': score['total'], **score}
                    signals.append(sig)

            # F: 눌림목 — 전일 시그널 + 오늘 MA5 터치
            if 'F' in active_modules:
                pb = _detect_pullback(df, i)
                if pb:
                    sig = {**base_info, **fwd, 'module': 'F', **pb}
                    signals.append(sig)

            # G: K값 변동성 돌파
            if 'G' in active_modules:
                kv = _detect_k_breakout(df, i)
                if kv:
                    sig = {**base_info, **fwd, 'module': 'G', **kv}
                    signals.append(sig)

            # H: RSI 반전 (Cameron)
            if 'H' in active_modules:
                rsi_sig = _detect_rsi_reversal(df, i)
                if rsi_sig:
                    sig = {**base_info, **fwd, 'module': 'H', **rsi_sig}
                    signals.append(sig)

            # I: 유목민 거감음봉
            if 'I' in active_modules:
                nomad = _detect_nomad_pattern(df, i)
                if nomad:
                    sig = {**base_info, **fwd, 'module': 'I', **nomad}
                    signals.append(sig)

            # J: 파동 OBV
            if 'J' in active_modules:
                wave = _detect_wave_obv(df, i)
                if wave:
                    sig = {**base_info, **fwd, 'module': 'J', **wave}
                    signals.append(sig)

        return signals

    except Exception as e:
        return []


# ══════════════════════════════════════════════
# 모듈별 시그널 감지 함수 (스텁 — 각각 구현 필요)
# ══════════════════════════════════════════════

def _calc_score_v38(df, i):
    """모듈 D: 현행 v3.8 9지표 100점제 점수 계산"""
    row = df.iloc[i]
    try:
        cci = row.get('cci', np.nan)
        rsi = row.get('rsi', np.nan)
        disp5 = row.get('disparity5', np.nan)
        disp20 = row.get('disparity20', np.nan)
        slope = row.get('ma20_slope', np.nan)
        vol_r = row.get('vol_ratio20', np.nan)
        change = row.get('change_rate', np.nan)
        consec = row.get('consec_bullish', 0)
        body = row.get('candle_body_pct', np.nan)

        if pd.isna(cci) or pd.isna(rsi):
            return None

        # CCI 점수 (22점) — 160~180 최적
        if 160 <= cci <= 180:
            cci_score = 22
        elif 140 <= cci < 160:
            cci_score = 18
        elif 180 < cci <= 220:
            cci_score = 15
        elif 100 <= cci < 140:
            cci_score = 10
        elif cci > 220:
            cci_score = 5  # 과열 감점
        else:
            cci_score = 0

        # 이격도 점수 (18점)
        if not pd.isna(disp20):
            if 2 <= disp20 <= 8:
                disp_score = 18
            elif 0 <= disp20 < 2:
                disp_score = 12
            elif 8 < disp20 <= 15:
                disp_score = 10
            else:
                disp_score = 0
        else:
            disp_score = 0

        # 등락률 점수 (15점)
        if not pd.isna(change):
            if 2 <= change <= 8:
                change_score = 15
            elif 0.5 <= change < 2:
                change_score = 10
            elif 8 < change <= 15:
                change_score = 8
            elif change > 15:
                change_score = 3  # 과열
            else:
                change_score = 0
        else:
            change_score = 0

        # MA20 기울기 점수 (20점)
        if not pd.isna(slope):
            if slope > 0.5:
                slope_score = 20
            elif slope > 0.1:
                slope_score = 15
            elif slope > 0:
                slope_score = 10
            else:
                slope_score = 0
        else:
            slope_score = 0

        # RSI 점수 (5점)
        if not pd.isna(rsi):
            if 40 <= rsi <= 65:
                rsi_score = 5
            elif 30 <= rsi < 40 or 65 < rsi <= 75:
                rsi_score = 3
            else:
                rsi_score = 0
        else:
            rsi_score = 0

        # 거래량비 점수 (10점)
        if not pd.isna(vol_r):
            if 1.5 <= vol_r <= 5:
                vol_score = 10
            elif 1 <= vol_r < 1.5:
                vol_score = 5
            elif vol_r > 5:
                vol_score = 7
            else:
                vol_score = 0
        else:
            vol_score = 0

        # 캔들 점수 (5점)
        is_bull = row.get('is_bullish', 0)
        candle_score = 5 if is_bull else 0

        # 과열 감점
        overheat = 0
        if not pd.isna(cci) and cci > 250:
            overheat -= 10
        if not pd.isna(change) and change > 20:
            overheat -= 10
        if consec >= 5:
            overheat -= 5

        total = max(0, cci_score + disp_score + change_score + slope_score +
                    rsi_score + vol_score + candle_score + overheat)

        return {
            'total': total,
            'cci_score': cci_score,
            'disp_score': disp_score,
            'change_score': change_score,
            'slope_score': slope_score,
            'rsi_score': rsi_score,
            'vol_score': vol_score,
            'candle_score': candle_score,
            'overheat': overheat,
            'cci_val': cci,
            'rsi_val': rsi,
        }
    except:
        return None


def _detect_pullback(df, i):
    """모듈 F: 눌림목 감지 — 직전 2~3일 내 거래량 폭발 후 오늘 MA5 터치"""
    try:
        row = df.iloc[i]
        if i < 5:
            return None

        # 직전 3일 중 거래량 폭발일이 있는지
        has_vol_spike = False
        spike_day = None
        for d in range(1, 4):
            if i - d < 0:
                break
            prev = df.iloc[i - d]
            if prev.get('vol_ratio20', 0) >= 3.0 and prev.get('change_rate', 0) >= 5.0:
                has_vol_spike = True
                spike_day = d
                break

        if not has_vol_spike:
            return None

        # 오늘 눌림목 조건: MA5 근접 + 음봉 또는 작은 양봉
        ma5 = row.get('ma5', np.nan)
        close = row['close']
        if pd.isna(ma5) or ma5 <= 0:
            return None

        ma5_touch = abs(close / ma5 - 1) <= 0.01  # MA5에서 1% 이내

        if not ma5_touch:
            return None

        return {
            'pullback_type': f'D-{spike_day}_spike_ma5_touch',
            'spike_day': spike_day,
            'ma5_dist': round((close / ma5 - 1) * 100, 2),
        }
    except:
        return None


def _detect_k_breakout(df, i):
    """모듈 G: K값 변동성 돌파"""
    try:
        row = df.iloc[i]
        bp = row.get('breakout_price', np.nan)
        if pd.isna(bp):
            return None

        # 고가가 돌파가 이상 + 거래대금 조건
        if row['high'] >= bp and row.get('trade_value', 0) >= 10_000_000_000:  # 100억+
            vol_r = row.get('vol_ratio20', 0)
            return {
                'breakout_price': round(bp, 0),
                'trade_value_b': round(row['trade_value'] / 1e8, 1),
                'vol_ratio': round(vol_r, 2),
            }
        return None
    except:
        return None


def _detect_rsi_reversal(df, i):
    """모듈 H: RSI 반전 (Ross Cameron 3중 교집합)"""
    try:
        row = df.iloc[i]
        rsi = row.get('rsi', np.nan)
        if pd.isna(rsi):
            return None

        # 조건 1: RSI ≤ 30 (과매도)
        if rsi > 30:
            return None

        # 조건 2: BB 하단 터치
        bb_lower = row.get('bb_lower', np.nan)
        if pd.isna(bb_lower) or row['low'] > bb_lower:
            return None

        # 조건 3: 반전 캔들 (양봉 + 아랫꼬리)
        is_bull = row.get('is_bullish', 0)
        lower_shadow = row.get('lower_shadow', 0)
        if not is_bull or lower_shadow < 0.3:
            return None

        # 확인: 전일 RSI가 오늘보다 높았는지 (RSI 반등 시작)
        prev_rsi = df.iloc[i-1].get('rsi', np.nan) if i > 0 else np.nan
        rsi_turning = not pd.isna(prev_rsi) and rsi > prev_rsi

        return {
            'rsi_val': round(rsi, 1),
            'bb_touch': True,
            'reversal_candle': True,
            'rsi_turning': rsi_turning,
        }
    except:
        return None


def _detect_nomad_pattern(df, i):
    """모듈 I: 유목민 거감음봉 패턴"""
    try:
        row = df.iloc[i]
        if i < 20:
            return None

        # 조건 1: 역사적 저점 근접 (120일 저가 대비 5% 이내)
        low_120 = df.iloc[max(0, i-120):i+1]['low'].min()
        if row['close'] > low_120 * 1.05:
            return None

        # 조건 2: 거래량 급감 (5일 평균이 20일 평균의 50% 이하)
        vol_ma5 = row.get('vol_ma5', np.nan)
        vol_ma20 = row.get('vol_ma20', np.nan)
        if pd.isna(vol_ma5) or pd.isna(vol_ma20) or vol_ma20 <= 0:
            return None
        vol_decline = vol_ma5 / vol_ma20
        if vol_decline > 0.5:
            return None

        # 조건 3: 음봉 (거감음봉)
        is_bearish = not row.get('is_bullish', True)

        return {
            'low_120_dist': round((row['close'] / low_120 - 1) * 100, 2),
            'vol_decline_ratio': round(vol_decline, 3),
            'is_bearish': is_bearish,
            'pattern': 'nomad_gergam' if is_bearish else 'nomad_low_vol',
        }
    except:
        return None


def _detect_wave_obv(df, i):
    """모듈 J: OBV 파동 감지"""
    try:
        row = df.iloc[i]
        obv = row.get('obv', np.nan)
        obv_ma = row.get('obv_ma20', np.nan)
        if pd.isna(obv) or pd.isna(obv_ma):
            return None

        # OBV가 20일 평균 상향 돌파 (bull signal)
        prev_obv = df.iloc[i-1].get('obv', np.nan) if i > 0 else np.nan
        prev_obv_ma = df.iloc[i-1].get('obv_ma20', np.nan) if i > 0 else np.nan

        if pd.isna(prev_obv) or pd.isna(prev_obv_ma):
            return None

        # 어제 OBV < MA20, 오늘 OBV >= MA20 (골든크로스)
        if prev_obv < prev_obv_ma and obv >= obv_ma:
            return {
                'obv_cross': 'bull',
                'obv_val': round(obv, 0),
                'obv_ma_val': round(obv_ma, 0),
            }
        return None
    except:
        return None


# ══════════════════════════════════════════════
# 일자별 처리 (거래량 TOP5 등 순위 기반 모듈)
# ══════════════════════════════════════════════

def _get_option_expiry_dates(trading_dates: list) -> set:
    """매월 둘째 목요일(옵션만기일) 근사 계산."""
    expiry = set()
    seen_months = set()
    for d in trading_dates:
        ts = pd.Timestamp(d)
        ym = (ts.year, ts.month)
        if ym in seen_months:
            continue
        first = ts.replace(day=1)
        dow = first.dayofweek
        first_thu = first + pd.Timedelta(days=(3 - dow) % 7)
        second_thu = first_thu + pd.Timedelta(days=7)
        expiry.add(second_thu.strftime('%Y-%m-%d'))
        seen_months.add(ym)
    return expiry


def run_daily_modules(data: dict, active_modules: set, config: dict) -> list:
    """
    모듈 A, B, C, E, K, L: 일자별 전종목 순위가 필요한 모듈 (12모듈 완성본).
    """
    from data_loader import calc_forward_returns
    hold_days = config.get('hold_days', (1,2,3,5,7,10,15,20))
    start_date = config.get('start_date')
    end_date = config.get('end_date')
    signals = []

    all_dates = set()
    for df in data.values():
        all_dates.update(df['date'].dt.strftime('%Y-%m-%d').tolist())
    trading_dates = sorted(all_dates)

    option_expiry = _get_option_expiry_dates(trading_dates)

    # 모듈 E: 기존 D 결과에서 일자별 TOP3 로드
    d_scores = {}
    if 'E' in active_modules:
        mod_d_path = Path(config['results_dir']) / "mod_D_signals.csv"
        if mod_d_path.exists():
            print("  모듈 E: mod_D_signals.csv 로드...")
            d_df = pd.read_csv(mod_d_path, dtype={'code': str}, encoding='utf-8-sig')
            for dt, grp in d_df.groupby('date'):
                top3 = grp.nlargest(3, 'score')
                d_scores[dt] = top3.to_dict('records')
            print(f"  -> {len(d_scores)}거래일 D 점수 로드")
        else:
            print("  !! 모듈 E: mod_D_signals.csv 없음. 먼저 --module D 실행 필요")

    print(f"  일자별 처리: {len(trading_dates)}거래일")

    for idx, date in enumerate(trading_dates):
        if idx < 60 or idx >= len(trading_dates) - 20:
            continue
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue

        daily = []
        for code, df in data.items():
            mask = df['date'] == date
            if mask.sum() == 0:
                continue
            daily.append(df[mask].iloc[0].to_dict())
        if len(daily) < 10:
            continue

        daily_df = pd.DataFrame(daily)
        daily_df = daily_df.sort_values('volume', ascending=False)
        daily_df['vol_rank'] = range(1, len(daily_df) + 1)
        daily_df = daily_df.sort_values('trade_value', ascending=False)
        daily_df['val_rank'] = range(1, len(daily_df) + 1)
        if 'change_rate' in daily_df.columns:
            daily_df = daily_df.sort_values('change_rate', ascending=False)
            daily_df['change_rank'] = range(1, len(daily_df) + 1)

        leading_sectors = []
        if 'sector' in daily_df.columns and 'change_rate' in daily_df.columns:
            sa = daily_df.groupby('sector')['change_rate'].mean()
            if len(sa) > 0:
                leading_sectors = sa.nlargest(3).index.tolist()

        def _fwd(code):
            if code not in data:
                return {}
            df = data[code]
            m = df[df['date'] == date]
            if len(m) == 0:
                return {}
            return calc_forward_returns(df, m.index[0], hold_days)

        def _base(r):
            return {
                'code': r['code'], 'name': r.get('name',''), 'date': date,
                'year': int(date[:4]), 'close': r['close'],
                'stock_type': r.get('stock_type',''),
                'is_preferred': r.get('is_preferred', False),
                'market': r.get('market',''),
            }

        # ── A: 거래량 TOP5 기본 ──
        if 'A' in active_modules:
            for _, r in daily_df.nsmallest(5, 'vol_rank').iterrows():
                fwd = _fwd(r['code'])
                if fwd:
                    signals.append({**_base(r), **fwd, 'module': 'A',
                        'vol_rank': int(r['vol_rank']), 'volume': r['volume'],
                        'trade_value': r.get('trade_value',0),
                        'change_rate': r.get('change_rate',0)})

        # ── B: 거래량 TOP5 + 필터 비교 ──
        if 'B' in active_modules:
            for _, r in daily_df.nsmallest(5, 'vol_rank').iterrows():
                fwd = _fwd(r['code'])
                if not fwd:
                    continue
                tv = r.get('trade_value', 0)
                cr = r.get('change_rate', 0)
                sect = r.get('sector', '')
                vr = r.get('vol_ratio20', 0)
                signals.append({**_base(r), **fwd, 'module': 'B',
                    'vol_rank': int(r['vol_rank']), 'trade_value': tv,
                    'change_rate': cr, 'sector': sect,
                    'is_leading_sector': int(sect in leading_sectors),
                    'f_tv100': int(tv >= 1e10), 'f_tv500': int(tv >= 5e10),
                    'f_leading': int(sect in leading_sectors),
                    'f_rank23': int(int(r['vol_rank']) in (2,3)),
                    'f_volr3': int(vr >= 3.0),
                    'f_cr20': int(cr >= 20), 'f_cr30': int(cr >= 30)})

        # ── C: 거래량 TOP5 + 보조지표 ──
        if 'C' in active_modules:
            for _, r in daily_df.nsmallest(5, 'vol_rank').iterrows():
                fwd = _fwd(r['code'])
                if not fwd:
                    continue
                signals.append({**_base(r), **fwd, 'module': 'C',
                    'vol_rank': int(r['vol_rank']),
                    'cci': r.get('cci', np.nan), 'rsi': r.get('rsi', np.nan),
                    'disparity20': r.get('disparity20', np.nan),
                    'ma20_slope': r.get('ma20_slope', np.nan),
                    'vol_ratio20': r.get('vol_ratio20', np.nan),
                    'consec_bullish': r.get('consec_bullish', 0),
                    'candle_body_pct': r.get('candle_body_pct', np.nan),
                    'is_bullish': r.get('is_bullish', 0),
                    'bb_pctb': r.get('bb_pctb', np.nan),
                    'change_rate': r.get('change_rate', 0),
                    'trade_value': r.get('trade_value', 0)})

        # ── E: 점수제 TOP3 순위별 보유기간 ──
        if 'E' in active_modules and date in d_scores:
            for ri, rec in enumerate(d_scores[date], 1):
                code = str(rec.get('code','')).zfill(6)
                fwd = _fwd(code)
                if fwd:
                    signals.append({'module': 'E', 'code': code,
                        'name': rec.get('name',''), 'date': date,
                        'year': int(date[:4]), 'close': rec.get('close',0),
                        'stock_type': rec.get('stock_type',''),
                        'is_preferred': rec.get('is_preferred', False),
                        'score': rec.get('score', 0), 'score_rank': ri, **fwd})

        # ── K: 시황/캘린더 ──
        if 'K' in active_modules:
            is_exp = date in option_expiry
            dow = pd.Timestamp(date).dayofweek
            mc = daily_df['change_rate'].median() if 'change_rate' in daily_df.columns else 0
            mv = daily_df['vol_ratio20'].median() if 'vol_ratio20' in daily_df.columns else 1
            top1 = daily_df.nsmallest(1, 'vol_rank')
            if len(top1) > 0:
                r = top1.iloc[0]
                fwd = _fwd(r['code'])
                if fwd:
                    signals.append({**_base(r), **fwd, 'module': 'K',
                        'is_option_expiry': int(is_exp),
                        'is_monday': int(dow==0), 'is_friday': int(dow==4),
                        'is_crash_day': int(mc < -2.0),
                        'is_rally_day': int(mc > 2.0),
                        'mkt_change': round(mc, 2), 'mkt_vol': round(mv, 2)})

        # ── L: 150억봉 추격 ──
        if 'L' in active_modules:
            big = daily_df[daily_df['trade_value'] >= 1.5e10]
            if 'is_bullish' in daily_df.columns:
                big = big[big['is_bullish'] == 1]
            for _, r in big.iterrows():
                fwd = _fwd(r['code'])
                if fwd:
                    signals.append({**_base(r), **fwd, 'module': 'L',
                        'trade_value': r.get('trade_value',0),
                        'trade_value_b': round(r.get('trade_value',0)/1e8, 1),
                        'change_rate': r.get('change_rate',0),
                        'vol_ratio20': r.get('vol_ratio20',0),
                        'vol_rank': int(r.get('vol_rank',0))})

        if idx % 250 == 0:
            print(f"    ... {date} ({idx}/{len(trading_dates)})")

    return signals


# ══════════════════════════════════════════════
# 메인 오케스트레이터
# ══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description='ClosingBell 종합 백테스트')
    parser.add_argument('--all', action='store_true', help='전체 모듈 실행')
    parser.add_argument('--module', nargs='+', default=[], help='실행할 모듈 (A B C D E F G H I J K L)')
    parser.add_argument('--workers', type=int, default=None, help='병렬 워커 수')
    parser.add_argument('--ohlcv', default=DEFAULT_CONFIG['ohlcv_dir'])
    parser.add_argument('--whitelist', default=DEFAULT_CONFIG['whitelist_csv'])
    parser.add_argument('--start', default=None, help='시작일 (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='종료일 (YYYY-MM-DD)')
    parser.add_argument('--report', action='store_true', help='기존 결과로 리포트만 생성')
    args = parser.parse_args()

    config = DEFAULT_CONFIG.copy()
    config['ohlcv_dir'] = args.ohlcv
    config['whitelist_csv'] = args.whitelist
    config['start_date'] = args.start
    config['end_date'] = args.end

    n_workers = args.workers or max(1, mp.cpu_count() - 1)

    # 결과 디렉토리
    results_dir = Path(config['results_dir'])
    results_dir.mkdir(parents=True, exist_ok=True)

    if args.report:
        generate_report(results_dir)
        return

    # 활성 모듈
    if args.all:
        active = set(MODULE_REGISTRY.keys())
    else:
        active = set(m.upper() for m in args.module)

    if not active:
        print("실행할 모듈을 지정하세요: --all 또는 --module D F E")
        return

    print("=" * 70)
    print("🚀 ClosingBell 종합 백테스트")
    print("=" * 70)
    print(f"활성 모듈: {', '.join(sorted(active))}")
    print(f"워커 수: {n_workers}")
    print(f"기간: {config['start_date'] or '전체'} ~ {config['end_date'] or '현재'}")
    print()

    t0 = time.time()

    # ── 1. 데이터 로딩 ──
    print("[1/4] 데이터 로딩...")
    loader = OHLCVLoader(
        ohlcv_dir=config['ohlcv_dir'],
        whitelist_csv=config['whitelist_csv'],
        n_workers=n_workers,
    )
    # 우선주 포함 (나중에 분리 분석)
    data = loader.load_all(exclude_preferred=False)
    print(f"  → {len(data)}종목 로드 ({time.time()-t0:.1f}초)")

    # ── 2. 종목별 지표 계산 + 시그널 감지 (병렬) ──
    print("\n[2/4] 종목별 지표 계산 & 시그널 감지 (병렬)...")
    t1 = time.time()

    # 종목별 병렬 처리 대상 모듈 (일자별 순위가 필요 없는 것)
    stock_modules = active & {'D', 'F', 'G', 'H', 'I', 'J'}

    all_signals = []

    if stock_modules:
        tasks = [(code, df, stock_modules, config) for code, df in data.items()]
        print(f"  종목별 처리: {len(tasks)}종목, 모듈: {stock_modules}")

        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            futures = [executor.submit(_process_stock, t) for t in tasks]
            done = 0
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_signals.extend(result)
                done += 1
                if done % 500 == 0:
                    print(f"    ... {done}/{len(tasks)} 종목 완료, 시그널 {len(all_signals):,}개")

        print(f"  → 종목별 시그널: {len(all_signals):,}개 ({time.time()-t1:.1f}초)")

    # ── 3. 일자별 처리 (순위 기반 모듈) ──
    daily_modules = active & {'A', 'B', 'C', 'E', 'K', 'L'}
    if daily_modules:
        print(f"\n[3/4] 일자별 처리: 모듈 {daily_modules}...")
        t2 = time.time()

        # 지표 미리 계산
        print("  지표 사전 계산 중...")
        precomputed = {}
        for code, df in data.items():
            precomputed[code] = precompute_indicators(df)

        daily_signals = run_daily_modules(precomputed, daily_modules, config)
        all_signals.extend(daily_signals)
        print(f"  → 일자별 시그널: {len(daily_signals):,}개 ({time.time()-t2:.1f}초)")
    else:
        print("\n[3/4] 일자별 모듈 없음 — 스킵")

    # ── 4. 결과 저장 & 리포트 ──
    print(f"\n[4/4] 결과 저장...")

    if not all_signals:
        print("❌ 시그널 없음")
        return

    results_df = pd.DataFrame(all_signals)
    print(f"  총 시그널: {len(results_df):,}개")

    # 모듈별 분리 저장
    for module in sorted(results_df['module'].unique()):
        mod_df = results_df[results_df['module'] == module]
        mod_path = results_dir / f"mod_{module}_signals.csv"
        mod_df.to_csv(mod_path, index=False, encoding='utf-8-sig')
        print(f"  📁 {mod_path.name}: {len(mod_df):,}건")

    # 전체 저장
    all_path = results_dir / "all_signals.csv"
    results_df.to_csv(all_path, index=False, encoding='utf-8-sig')

    # 요약 리포트
    generate_report(results_dir, results_df)

    elapsed = time.time() - t0
    print(f"\n✅ 완료! 총 소요: {elapsed/60:.1f}분 ({elapsed/3600:.1f}시간)")


def generate_report(results_dir: Path, results_df: pd.DataFrame = None):
    """종합 리포트 생성"""
    if results_df is None:
        all_path = results_dir / "all_signals.csv"
        if not all_path.exists():
            print("결과 파일 없음")
            return
        results_df = pd.read_csv(all_path)

    hold_days = (1, 2, 3, 5, 7, 10, 15, 20)

    print("\n" + "=" * 80)
    print("📊 종합 백테스트 리포트")
    print("=" * 80)

    # 모듈별 요약
    print(f"\n{'모듈':<4} {'이름':<20} {'시그널':>8} ", end='')
    for d in hold_days:
        print(f"{'D+'+str(d):>8}", end='')
    print()
    print("-" * (36 + 8 * len(hold_days)))

    for module in sorted(results_df['module'].unique()):
        mod_df = results_df[results_df['module'] == module]
        mod_name = MODULE_REGISTRY.get(module, {}).get('name', module)
        print(f"  {module}  {mod_name:<20} {len(mod_df):>8,} ", end='')
        for d in hold_days:
            col = f'D+{d}_winrate' if f'D+{d}_return' in mod_df.columns else None
            ret_col = f'D+{d}_return'
            if ret_col in mod_df.columns:
                wr = mod_df[f'D+{d}_win'].dropna().mean() * 100 if f'D+{d}_win' in mod_df.columns else np.nan
                print(f"{wr:>7.1f}%", end='')
            else:
                print(f"{'N/A':>8}", end='')
        print()

    # 보통주 vs 우선주 비교
    if 'is_preferred' in results_df.columns:
        print(f"\n\n📊 보통주 vs 우선주 비교")
        print("-" * 50)
        for stype, label in [(False, '보통주'), (True, '우선주')]:
            sdf = results_df[results_df['is_preferred'] == stype]
            if len(sdf) == 0:
                continue
            d3_wr = sdf['D+3_win'].dropna().mean() * 100 if 'D+3_win' in sdf.columns else 0
            d5_wr = sdf['D+5_win'].dropna().mean() * 100 if 'D+5_win' in sdf.columns else 0
            print(f"  {label}: {len(sdf):,}건, D+3 승률 {d3_wr:.1f}%, D+5 승률 {d5_wr:.1f}%")

    # 연도별 (모듈 D 기준)
    if 'year' in results_df.columns:
        print(f"\n\n📊 연도별 추이 (전체 모듈)")
        print("-" * 50)
        for year in sorted(results_df['year'].unique()):
            ydf = results_df[results_df['year'] == year]
            d3_wr = ydf['D+3_win'].dropna().mean() * 100 if 'D+3_win' in ydf.columns else 0
            print(f"  {year}: {len(ydf):,}건, D+3 승률 {d3_wr:.1f}%")

    # CSV 리포트
    summary = summarize_results(results_df, hold_days, group_by='module')
    summary.to_csv(results_dir / "summary_by_module.csv", index=False)

    if 'year' in results_df.columns:
        yearly = summarize_results(results_df, hold_days, group_by='year')
        yearly.to_csv(results_dir / "summary_by_year.csv", index=False)

    if 'is_preferred' in results_df.columns:
        results_df['pref_label'] = results_df['is_preferred'].map({True: 'preferred', False: 'common'})
        pref = summarize_results(results_df, hold_days, group_by='pref_label')
        pref.to_csv(results_dir / "summary_preferred_vs_common.csv", index=False)

    print(f"\n📁 리포트 저장: {results_dir}/")


if __name__ == "__main__":
    main()
