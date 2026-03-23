"""
universe_filter.py — ClosingBell 파이프라인용 유니버스 필터 모듈
================================================================
generate_universe.py로 생성한 CSV를 기반으로
기존 파이프라인의 OHLCV 수집/스크리닝 단계에서 종목 필터링

사용법 (ClosingBell 파이프라인 내에서):
    from universe_filter import UniverseFilter

    uf = UniverseFilter()  # universe_whitelist.csv 자동 로드
    
    # 방법 1: 종목코드 하나 체크
    if uf.is_valid("244880"):  # 나눔테크 → False (코넥스)
        ...
    
    # 방법 2: 종목코드 리스트 필터링
    all_codes = ["005930", "244880", "069500", ...]
    valid_codes = uf.filter(all_codes)
    
    # 방법 3: DataFrame 필터링 (OHLCV 수집 후)
    df_ohlcv = uf.filter_df(df_ohlcv, code_col="종목코드")
"""

import os
import pandas as pd
from datetime import datetime, timedelta


class UniverseFilter:
    """유니버스 화이트리스트 기반 종목 필터"""

    def __init__(self, csv_path="universe_whitelist.csv", auto_warn_days=30):
        """
        Args:
            csv_path: universe_whitelist.csv 경로
            auto_warn_days: CSV가 이 일수보다 오래되면 갱신 경고
        """
        self.csv_path = csv_path
        self._whitelist = set()
        self._df = None
        self._load(auto_warn_days)

    def _load(self, warn_days):
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(
                f"❌ {self.csv_path} 없음. "
                f"먼저 python generate_universe.py 실행 필요"
            )

        self._df = pd.read_csv(self.csv_path, dtype={'Code': str})
        self._df['Code'] = self._df['Code'].str.zfill(6)
        self._whitelist = set(self._df['Code'].tolist())

        # 파일 수정일 체크 → 오래되면 경고
        mtime = datetime.fromtimestamp(os.path.getmtime(self.csv_path))
        age_days = (datetime.now() - mtime).days
        if age_days > warn_days:
            print(f"⚠️ universe_whitelist.csv가 {age_days}일 전 생성됨. "
                  f"python generate_universe.py 재실행 권장")

        print(f"📋 유니버스 로드: {len(self._whitelist)}종목 "
              f"(생성: {mtime.strftime('%Y-%m-%d')})")

    def is_valid(self, code: str) -> bool:
        """종목코드가 유니버스에 포함되는지 확인"""
        return str(code).zfill(6) in self._whitelist

    def filter(self, codes: list) -> list:
        """종목코드 리스트에서 유효한 것만 반환"""
        return [c for c in codes if self.is_valid(c)]

    def filter_df(self, df: pd.DataFrame, code_col: str = "Code") -> pd.DataFrame:
        """DataFrame에서 유니버스에 포함된 종목만 필터링"""
        mask = df[code_col].astype(str).str.zfill(6).isin(self._whitelist)
        removed = len(df) - mask.sum()
        if removed > 0:
            print(f"  🔽 유니버스 필터: {removed}종목 제외, {mask.sum()}종목 통과")
        return df[mask].copy()

    def get_excluded(self, codes: list) -> list:
        """유니버스에서 제외된 종목코드 반환 (디버깅용)"""
        return [c for c in codes if not self.is_valid(c)]

    def get_info(self, code: str) -> dict:
        """종목 정보 반환"""
        code = str(code).zfill(6)
        match = self._df[self._df['Code'] == code]
        if len(match) == 0:
            return None
        row = match.iloc[0]
        return row.to_dict()

    @property
    def codes(self) -> set:
        """유니버스 전체 코드셋"""
        return self._whitelist

    @property
    def count(self) -> int:
        return len(self._whitelist)

    def add_code(self, code: str, name: str, market: str):
        """
        신규 상장사 수동 추가 (CSV에도 반영)
        
        사용법: uf.add_code("999999", "신규기업", "KOSDAQ")
        """
        code = str(code).zfill(6)
        if code in self._whitelist:
            print(f"이미 존재: {code}")
            return

        new_row = pd.DataFrame([{'Code': code, 'Name': name, 'Market': market}])
        self._df = pd.concat([self._df, new_row], ignore_index=True)
        self._whitelist.add(code)

        # CSV 갱신
        self._df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        print(f"✅ 추가: {code} ({name}) — {market}")

    def remove_code(self, code: str):
        """상장폐지 등으로 종목 제거"""
        code = str(code).zfill(6)
        if code not in self._whitelist:
            print(f"없는 코드: {code}")
            return

        name = self._df[self._df['Code'] == code]['Name'].values[0]
        self._df = self._df[self._df['Code'] != code]
        self._whitelist.discard(code)

        self._df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        print(f"🗑️ 제거: {code} ({name})")


# ─────────────────────────────────────────
# 파이프라인 적용 예시
# ─────────────────────────────────────────
"""
[기존 ClosingBell 파이프라인 수정 예시]

# === BEFORE (2,782종목 전체 스캔) ===
codes = get_all_krx_codes()  # 코넥스+ETF+스팩 섞임
for code in codes:
    ohlcv = fetch_ohlcv(code)
    ...

# === AFTER (순수 보통주만 스캔) ===
from universe_filter import UniverseFilter

uf = UniverseFilter("universe_whitelist.csv")
codes = get_all_krx_codes()
codes = uf.filter(codes)  # ← 이 한 줄 추가!
for code in codes:
    ohlcv = fetch_ohlcv(code)
    ...


[wave_monitor 파동감지에 적용]

# 파동 감지 결과 필터링
wave_signals = detect_waves(all_codes)
wave_signals = [s for s in wave_signals if uf.is_valid(s['code'])]


[월 1회 갱신 크론잡 — 매월 1일 새벽]
# crontab -e
# 0 5 1 * * cd /path/to/closingbell && python generate_universe.py
"""
