"""
PADO v2 재차거시 역사 백테스트
================================
과거 날짜별로 파이프라인 시뮬레이션 → D+1~D+5 수익률 측정.

시뮬 가능:
  ✅ 차트 (OHLCV 로컬 데이터)
  ✅ 거래량 (OHLCV 로컬 데이터)
  ✅ 시황 — 나스닥/코스피(global CSV) + 거래대금 변화율(OHLCV)
  ⚠️ 시황 — 키움 테마 API: 불가 → 소스2(변화율)만 사용
  ⚠️ 재료: DART/뉴스/Gemini 불가 → 중립(50점) 고정

의미:
  "차트+거래량+시황 타이밍"이 돈을 벌 수 있는 구조인지 검증.
  재료가 추가되면 성과가 더 좋아지는지는 실전 데이터로 측정.

사용법:
  python tools/jcgs_backtest.py                    # 최근 3개월
  python tools/jcgs_backtest.py --start 2025-06-01 # 시작일 지정
  python tools/jcgs_backtest.py --sample 500       # 종목 수 제한
"""

import sys
import os
import argparse
import time
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict, Counter

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import OHLCV_DIR, GLOBAL_CSV, MAPPING_CSV, JCGS_PASS, JCGS_FAIL, JCGS_WEIGHT, setup_logging

logger = setup_logging().getChild("jcgs_bt")

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

HOLD_DAYS = [1, 2, 3, 5]        # 수익률 측정 기간
LOOKBACK = 120                    # 시뮬 시작 전 필요 데이터
MAX_CANDIDATES_PER_DAY = 60       # 차트 스캔 최대 후보
NEUTRAL_MATERIAL_SCORE = 50       # 재료 중립 점수


# ─────────────────────────────────────────────
# 데이터 로드
# ─────────────────────────────────────────────

def load_universe(sample: int = 0) -> dict[str, pd.DataFrame]:
    """전종목 OHLCV 로드."""
    import csv
    codes = []
    if MAPPING_CSV.exists():
        with open(MAPPING_CSV, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code", row.get("종목코드", ""))).strip().zfill(6)
                if code:
                    codes.append(code)

    if sample > 0:
        import random
        random.seed(42)
        codes = random.sample(codes, min(sample, len(codes)))

    universe = {}
    for code in codes:
        p = OHLCV_DIR / f"{code}.csv"
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            df.columns = [c.strip().lower() for c in df.columns]
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)
            for c in ("open", "high", "low", "close"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
            df = df.dropna(subset=["close"])
            if len(df) >= LOOKBACK:
                universe[code] = df
        except Exception:
            pass

    return universe


def load_global() -> pd.DataFrame | None:
    """글로벌 지수 CSV 로드."""
    try:
        df = pd.read_csv(GLOBAL_CSV, parse_dates=["Date"])
        return df.sort_values("Date").reset_index(drop=True)
    except Exception:
        return None


def load_sector_map() -> dict[str, str]:
    """code → KSIC 섹터."""
    import csv
    result = {}
    if MAPPING_CSV.exists():
        with open(MAPPING_CSV, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code", row.get("종목코드", ""))).strip().zfill(6)
                sector = row.get("sector", row.get("섹터", "")).strip()
                if code:
                    result[code] = sector
    return result


# ─────────────────────────────────────────────
# 날짜별 시뮬레이션
# ─────────────────────────────────────────────

def get_trading_dates(universe: dict, start: str, end: str) -> list[str]:
    """공통 거래일 추출."""
    # 삼성전자(005930)의 날짜를 기준으로
    ref = universe.get("005930")
    if ref is None:
        ref = next(iter(universe.values()))
    dates = ref["date"].dt.strftime("%Y-%m-%d").tolist()
    return [d for d in dates if start <= d <= end]


def simulate_day(
    sim_date: str,
    universe: dict,
    global_df: pd.DataFrame | None,
    sector_map: dict,
) -> list[dict]:
    """하루 시뮬레이션 → 시그널 리스트."""
    from jaechageosi.chart_engine import ChartEngine
    from jaechageosi.volume_engine import VolumeEngine
    from shared.theme_taxonomy import normalize_sector, merge_theme_sources

    sim_dt = pd.Timestamp(sim_date)
    signals = []

    # ── 1. 차트 스캔 ──
    chart_results = []
    for code, df in universe.items():
        mask = df["date"] <= sim_dt
        if mask.sum() < 60:
            continue
        sliced = df[mask].tail(LOOKBACK).reset_index(drop=True)

        engine = ChartEngine.__new__(ChartEngine)
        engine.ohlcv_dir = OHLCV_DIR
        engine.stock_map = {}

        try:
            # 직접 score_single 로직 실행 (df 전달)
            sigs = engine._detect_signals(sliced)
            if not sigs:
                continue
            state = engine._classify_state(sliced)
            alignment = engine._calc_ma_alignment(sliced)
            rsi = engine._calc_rsi(sliced)
            gc_ago = engine._gc_days_ago(sliced)
            score, reasons = engine._score_breakdown(sliced, sigs, alignment, rsi, gc_ago)
            if score >= 25:
                chart_results.append({
                    "code": code, "score": score, "signals": sigs,
                    "state": state, "rsi": rsi,
                })
        except Exception:
            continue

    chart_results.sort(key=lambda x: x["score"], reverse=True)
    chart_results = chart_results[:MAX_CANDIDATES_PER_DAY]

    if not chart_results:
        return []

    # ── 2. 거래량 ──
    vol_pass = []
    for cr in chart_results:
        code = cr["code"]
        df = universe[code]
        sliced = df[df["date"] <= sim_dt].tail(LOOKBACK).reset_index(drop=True)

        engine = VolumeEngine.__new__(VolumeEngine)
        engine.ohlcv_dir = OHLCV_DIR
        engine.api = None

        try:
            vr = engine.score_single(code, df=sliced)
            if vr and vr.score >= 20:
                vol_pass.append({"chart": cr, "volume": vr})
        except Exception:
            continue

    if not vol_pass:
        return []

    # ── 3. 시황 (거래대금 변화율 + 글로벌) ──
    # 3a. 거래대금 변화율 → 주도섹터
    tv_sectors = []
    for code, df in universe.items():
        sliced = df[df["date"] <= sim_dt].tail(25)
        if len(sliced) < 22:
            continue
        try:
            tv = sliced["close"] * sliced["volume"]
            tv_today = float(tv.iloc[-1])
            tv_ma20 = float(tv.iloc[-21:-1].mean())
            if tv_ma20 > 0 and tv_today >= 3e9:
                change = (tv_today - tv_ma20) / tv_ma20 * 100
                if change >= 100:
                    sector = sector_map.get(code, "")
                    canon = normalize_sector(sector) if sector else None
                    if canon:
                        tv_sectors.append(canon)
        except Exception:
            continue

    sector_counts = Counter(tv_sectors)
    change_sectors = [c for c, _ in sector_counts.most_common(3)]

    # 3b. 나스닥 변화율
    nasdaq_chg = 0.0
    if global_df is not None:
        g_mask = global_df["Date"] <= sim_dt
        g_slice = global_df[g_mask]
        if "NASDAQ" in g_slice.columns and len(g_slice) >= 2:
            nq = g_slice["NASDAQ"].dropna()
            if len(nq) >= 2:
                nasdaq_chg = (nq.iloc[-1] - nq.iloc[-2]) / nq.iloc[-2] * 100

    # 3c. 시황 점수 계산
    theme_score, themes = merge_theme_sources([], change_sectors, [])
    nasdaq_score = 20 if nasdaq_chg >= 1 else (12 if nasdaq_chg >= 0 else (6 if nasdaq_chg >= -1 else 0))
    calendar_score = 25  # 중립
    kospi_score = 15     # 중립
    market_score = min(nasdaq_score + calendar_score + theme_score + kospi_score, 100)

    # ── 4. 교집합 (간소화) ──
    for item in vol_pass:
        cr = item["chart"]
        vr = item["volume"]
        code = cr["code"]

        # 재료: 중립
        mat_score = NEUTRAL_MATERIAL_SCORE

        # 등급 판정
        verdicts = {
            "chart": "pass" if cr["score"] >= JCGS_PASS["chart"] else ("gray" if cr["score"] >= JCGS_FAIL["chart"] else "fail"),
            "volume": "pass" if vr.score >= JCGS_PASS["volume"] else ("gray" if vr.score >= JCGS_FAIL["volume"] else "fail"),
            "material": "pass" if mat_score >= JCGS_PASS["material"] else ("gray" if mat_score >= JCGS_FAIL["material"] else "fail"),
            "market": "pass" if market_score >= JCGS_PASS["market"] else ("gray" if market_score >= JCGS_FAIL["market"] else "fail"),
        }

        if any(v == "fail" for v in verdicts.values()):
            continue

        pass_count = sum(1 for v in verdicts.values() if v == "pass")
        if pass_count >= 3:
            grade = "A" if pass_count == 4 else "B"
        elif pass_count == 2:
            grade = "C"
        else:
            continue

        # 테마 매칭 보너스
        sector = sector_map.get(code, "")
        stock_canon = normalize_sector(sector)
        theme_bonus = 8 if stock_canon and stock_canon in themes else 0

        confidence = round(
            cr["score"] * JCGS_WEIGHT["chart"]
            + vr.score * JCGS_WEIGHT["volume"]
            + mat_score * JCGS_WEIGHT["material"]
            + market_score * JCGS_WEIGHT["market"]
            + theme_bonus
        )

        signals.append({
            "date": sim_date, "code": code, "grade": grade,
            "confidence": confidence,
            "chart_score": cr["score"], "volume_score": vr.score,
            "material_score": mat_score, "market_score": market_score,
            "theme_bonus": theme_bonus,
            "chart_state": cr["state"], "flow_state": vr.flow_state,
            "themes": themes,
        })

    return signals


def measure_returns(signals: list[dict], universe: dict) -> pd.DataFrame:
    """시그널별 D+1~D+5 수익률 측정."""
    rows = []
    for sig in signals:
        code = sig["code"]
        sig_date = pd.Timestamp(sig["date"])
        df = universe.get(code)
        if df is None:
            continue

        idx = df[df["date"] == sig_date].index
        if len(idx) == 0:
            continue
        i = idx[0]

        entry_price = df.iloc[i]["close"]
        row = {**sig, "entry_price": entry_price}

        for d in HOLD_DAYS:
            if i + d < len(df):
                exit_price = df.iloc[i + d]["close"]
                row[f"d{d}_ret"] = round((exit_price - entry_price) / entry_price * 100, 2)
            else:
                row[f"d{d}_ret"] = np.nan

        rows.append(row)

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PADO v2 재차거시 역사 백테스트")
    parser.add_argument("--start", type=str, default="", help="시작일 (기본: 3개월 전)")
    parser.add_argument("--end", type=str, default="", help="종료일 (기본: 최근)")
    parser.add_argument("--sample", type=int, default=0, help="종목 수 제한 (0=전종목)")
    parser.add_argument("--step", type=int, default=5, help="N거래일마다 스캔 (기본: 5=주1회)")
    args = parser.parse_args()

    end_date = args.end or (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    start_date = args.start or (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=90)).strftime("%Y-%m-%d")

    print("="*60)
    print(f"  PADO v2 재차거시 백테스트")
    print(f"  기간: {start_date} ~ {end_date}")
    print(f"  샘플: {'전종목' if args.sample == 0 else f'{args.sample}종목'}")
    print(f"  스캔 간격: {args.step}거래일")
    print("="*60)

    # 데이터 로드
    print("\n데이터 로드 중...")
    t0 = time.time()
    universe = load_universe(args.sample)
    global_df = load_global()
    sector_map = load_sector_map()
    print(f"  {len(universe)}종목 로드, {time.time()-t0:.1f}초")

    # 거래일 목록
    trading_dates = get_trading_dates(universe, start_date, end_date)
    # step 간격으로 샘플링
    scan_dates = trading_dates[::args.step]
    print(f"  거래일 {len(trading_dates)}일 중 {len(scan_dates)}일 스캔")

    # 날짜별 시뮬레이션
    all_signals = []
    for i, sim_date in enumerate(scan_dates):
        t1 = time.time()
        signals = simulate_day(sim_date, universe, global_df, sector_map)
        elapsed = time.time() - t1

        for s in signals:
            all_signals.append(s)

        a_cnt = sum(1 for s in signals if s["grade"] == "A")
        b_cnt = sum(1 for s in signals if s["grade"] == "B")
        themes = signals[0]["themes"] if signals else []

        print(f"  [{i+1}/{len(scan_dates)}] {sim_date}: A={a_cnt} B={b_cnt} "
              f"themes={themes} ({elapsed:.1f}초)")

    print(f"\n총 시그널: {len(all_signals)}건")

    if not all_signals:
        print("시그널 0건 — 종료")
        return

    # 수익률 측정
    print("\n수익률 측정 중...")
    results = measure_returns(all_signals, universe)

    if results.empty:
        print("수익률 데이터 없음 — 종료")
        return

    # ── 결과 출력 ──
    print("\n" + "="*60)
    print("  결과 요약")
    print("="*60)

    for grade in ["A", "B", "C"]:
        g = results[results["grade"] == grade]
        if g.empty:
            continue
        print(f"\n  [{grade}등급] {len(g)}건")
        for d in HOLD_DAYS:
            col = f"d{d}_ret"
            if col in g.columns:
                avg = g[col].dropna().mean()
                win = (g[col].dropna() > 0).mean() * 100
                print(f"    D+{d}: 평균 {avg:+.2f}%, 승률 {win:.1f}%")

    # 전체
    print(f"\n  [전체] {len(results)}건")
    for d in HOLD_DAYS:
        col = f"d{d}_ret"
        if col in results.columns:
            avg = results[col].dropna().mean()
            win = (results[col].dropna() > 0).mean() * 100
            median = results[col].dropna().median()
            print(f"    D+{d}: 평균 {avg:+.2f}%, 중간값 {median:+.2f}%, 승률 {win:.1f}%")

    # theme_bonus 유무별
    with_theme = results[results["theme_bonus"] > 0]
    without_theme = results[results["theme_bonus"] == 0]
    if len(with_theme) > 0 and len(without_theme) > 0:
        print(f"\n  [테마 매칭 효과]")
        for d in HOLD_DAYS:
            col = f"d{d}_ret"
            avg_w = with_theme[col].dropna().mean()
            avg_wo = without_theme[col].dropna().mean()
            print(f"    D+{d}: 매칭O {avg_w:+.2f}% ({len(with_theme)}건) vs 매칭X {avg_wo:+.2f}% ({len(without_theme)}건)")

    # CSV 저장
    output_path = Path("tools") / "jcgs_backtest_results.csv"
    results.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n  결과 저장: {output_path} ({len(results)}행)")


if __name__ == "__main__":
    main()
