"""
PADO v1 미니 백테스트
======================
chart_state + flow_state별 D+1~D+5 수익률 검증.
PC에서 실행: python tools/mini_backtest.py

전종목 OHLCV를 과거 날짜 기준으로 스캔하고,
시그널 발생 후 D+1~D+5 종가 대비 수익률 계산.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from config import OHLCV_DIR, MAPPING_CSV, setup_logging

logger = setup_logging().getChild("backtest")

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

LOOKBACK_DAYS = 120   # 시뮬 시작 전 필요 데이터
HOLD_DAYS = [1, 2, 3, 5]  # D+1 ~ D+5 수익률 측정
MAX_STOCKS = 0        # 0 = 전종목, 숫자 넣으면 샘플링

# 기간 설정 (기본: 최근 6개월)
# 전체 기간 하려면 START_DATE를 "2017-01-01" 등으로 변경
START_DATE = "2025-09-01"
END_DATE = "2026-03-20"


# ─────────────────────────────────────────────
# OHLCV 로드
# ─────────────────────────────────────────────

def load_ohlcv(code: str) -> pd.DataFrame | None:
    p = OHLCV_DIR / f"{code}.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
        df.columns = [c.strip().lower() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        for c in ("open", "high", "low", "close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
        return df.dropna(subset=["close"])
    except Exception:
        return None


# ─────────────────────────────────────────────
# 차트 분석 함수들 (chart_engine에서 추출)
# ─────────────────────────────────────────────

def calc_rsi(series: pd.Series, period: int = 14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    val = rsi.iloc[-1]
    return round(float(val), 1) if not np.isnan(val) else 50.0


def classify_chart_state(df_window: pd.DataFrame) -> str:
    """bottom/pullback/breakout/extended 분류."""
    last = df_window.iloc[-1]
    rsi = calc_rsi(df_window["close"])
    ma8 = df_window["close"].rolling(8).mean().iloc[-1]
    ma33 = df_window["close"].rolling(33).mean().iloc[-1]
    ma5 = df_window["close"].rolling(5).mean().iloc[-1]
    ma5_gap = (last["close"] - ma5) / last["close"] if last["close"] > 0 else 0
    high_20d = df_window["high"].tail(20).max()
    pullback = (high_20d - last["close"]) / high_20d if high_20d > 0 else 0

    if rsi > 70 and ma5_gap > 0.10:
        return "extended"
    if last["close"] > ma8 > ma33 and pullback < 0.03:
        return "breakout"
    if 0.03 <= pullback <= 0.12 and last["close"] >= ma33 * 0.98:
        return "pullback"
    return "bottom"


def classify_flow_state(df_window: pd.DataFrame) -> str:
    """accumulation/ignite/digest/reignite/chasing 분류."""
    if len(df_window) < 21:
        return "accumulation"

    last = df_window.iloc[-1]
    vol_ma20 = df_window["volume"].rolling(20).mean().iloc[-1]
    if vol_ma20 <= 0:
        return "accumulation"

    explosion = last["volume"] / vol_ma20

    # OBV 다이버전스
    recent = df_window.tail(20)
    price_slope = np.polyfit(range(min(20, len(recent))), recent["close"].values[-20:], 1)[0]
    obv = (np.sign(recent["close"].diff()) * recent["volume"]).cumsum()
    obv_slope = np.polyfit(range(len(obv)), obv.values, 1)[0]
    obv_div = price_slope < 0 and obv_slope > 0

    # 거래량 급감일
    threshold = vol_ma20 * 0.25
    dryup = 0
    for i in range(len(df_window) - 1, max(len(df_window) - 11, 0), -1):
        if df_window.iloc[i]["volume"] < threshold:
            dryup += 1
        else:
            break

    # 양봉 추격 패턴
    chase = last["close"] > last["open"] and explosion > 3

    # 윗꼬리 함정
    body = abs(last["close"] - last["open"])
    upper_wick = last["high"] - max(last["close"], last["open"])
    trap = body > 0 and upper_wick / body > 1.5 and chase

    if trap or (explosion > 3 and not obv_div and last["close"] > last["open"]):
        return "chasing"
    if obv_div and dryup >= 2 and explosion >= 2:
        return "reignite"
    if dryup >= 3 and explosion < 2:
        return "digest"
    if explosion >= 3:
        return "ignite"
    return "accumulation"


def detect_signals(df_window: pd.DataFrame) -> list[str]:
    """신호 탐지 (간소화)."""
    signals = []
    if len(df_window) < 45:
        return signals

    ma8 = df_window["close"].rolling(8).mean()
    ma33 = df_window["close"].rolling(33).mean()

    # GC(8→33) 최근 5일
    for i in range(-5, 0):
        idx = len(df_window) + i
        if idx < 1:
            continue
        if ma8.iloc[idx] > ma33.iloc[idx] and ma8.iloc[idx-1] <= ma33.iloc[idx-1]:
            signals.append("gc")
            break

    # RSI 30↓ 반등
    rsi_series = calc_rsi_series(df_window["close"])
    if len(rsi_series) >= 3:
        if rsi_series.iloc[-3] < 30 and rsi_series.iloc[-1] > 30:
            signals.append("rsi_reclaim")

    # 이평 배열 전환
    if len(df_window) >= 10:
        ma45_prev = df_window["close"].rolling(45).mean().iloc[-5]
        ma45_now = df_window["close"].rolling(45).mean().iloc[-1]
        if ma8.iloc[-5] < ma33.iloc[-5] and ma8.iloc[-1] > ma33.iloc[-1]:
            signals.append("reversal")

    # 거감음봉
    last = df_window.iloc[-1]
    prev_mean = df_window["volume"].tail(6).iloc[:-1].mean()
    if prev_mean > 0:
        vol_drop = last["volume"] < prev_mean * 0.25
        bearish = last["close"] < last["open"]
        ma5 = df_window["close"].rolling(5).mean().iloc[-1]
        gap = abs(last["close"] - ma5) / ma5 if ma5 > 0 else 1
        if vol_drop and bearish and gap <= 0.05:
            signals.append("gge")

    return signals


def calc_rsi_series(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


# ─────────────────────────────────────────────
# 백테스트 메인
# ─────────────────────────────────────────────

def run_backtest():
    print("=" * 60)
    print(f"PADO v1 미니 백테스트")
    print(f"기간: {START_DATE} ~ {END_DATE}")
    print(f"D+{HOLD_DAYS} 수익률 측정")
    print("=" * 60)

    # 종목 목록
    codes = []
    if MAPPING_CSV.exists():
        import csv
        with open(MAPPING_CSV, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = str(row.get("code", row.get("종목코드", ""))).strip().zfill(6)
                if code:
                    codes.append(code)
    if not codes:
        codes = [p.stem for p in OHLCV_DIR.glob("*.csv")]

    if MAX_STOCKS > 0:
        import random
        codes = random.sample(codes, min(MAX_STOCKS, len(codes)))

    print(f"대상: {len(codes)}종목")
    print()

    # 결과 수집
    results = []
    scan_dates = pd.bdate_range(START_DATE, END_DATE, freq="B")  # 영업일만

    processed = 0
    for code in codes:
        df = load_ohlcv(code)
        if df is None or len(df) < LOOKBACK_DAYS + max(HOLD_DAYS) + 10:
            continue

        processed += 1
        if processed % 200 == 0:
            print(f"  진행: {processed}/{len(codes)} ({len(results)}건 시그널)")

        for scan_date in scan_dates:
            # scan_date까지의 데이터만 사용 (미래 데이터 배제)
            mask = df["date"] <= scan_date
            df_window = df[mask].tail(LOOKBACK_DAYS)

            if len(df_window) < 60:
                continue

            # 신호 탐지
            signals = detect_signals(df_window)
            if not signals:
                continue

            # 상태 분류
            chart_state = classify_chart_state(df_window)
            flow_state = classify_flow_state(df_window)

            # D+N 수익률 계산
            future_mask = df["date"] > scan_date
            future = df[future_mask].head(max(HOLD_DAYS))

            if len(future) < max(HOLD_DAYS):
                continue

            entry_price = future.iloc[0]["open"]  # D+1 시가 진입
            if entry_price <= 0:
                continue

            returns = {}
            for d in HOLD_DAYS:
                if d <= len(future):
                    exit_price = future.iloc[d - 1]["close"]
                    returns[f"d{d}"] = round((exit_price - entry_price) / entry_price * 100, 2)

            results.append({
                "code": code,
                "date": scan_date.strftime("%Y-%m-%d"),
                "signals": ",".join(signals),
                "chart_state": chart_state,
                "flow_state": flow_state,
                **returns,
            })

    print(f"\n총 시그널: {len(results)}건")
    if not results:
        print("시그널 0건 — 기간이나 조건 조정 필요")
        return

    df_results = pd.DataFrame(results)

    # ─── 분석 ───

    print("\n" + "=" * 60)
    print("chart_state별 D+1~D+5 수익률")
    print("=" * 60)
    for state in ["bottom", "pullback", "breakout", "extended"]:
        sub = df_results[df_results["chart_state"] == state]
        if len(sub) == 0:
            continue
        print(f"\n  {state} ({len(sub)}건)")
        for d in HOLD_DAYS:
            col = f"d{d}"
            if col in sub.columns:
                avg = sub[col].mean()
                win = (sub[col] > 0).mean() * 100
                print(f"    D+{d}: 평균 {avg:+.2f}%  승률 {win:.1f}%")

    print("\n" + "=" * 60)
    print("flow_state별 D+1~D+5 수익률")
    print("=" * 60)
    for state in ["accumulation", "ignite", "digest", "reignite", "chasing"]:
        sub = df_results[df_results["flow_state"] == state]
        if len(sub) == 0:
            continue
        print(f"\n  {state} ({len(sub)}건)")
        for d in HOLD_DAYS:
            col = f"d{d}"
            if col in sub.columns:
                avg = sub[col].mean()
                win = (sub[col] > 0).mean() * 100
                print(f"    D+{d}: 평균 {avg:+.2f}%  승률 {win:.1f}%")

    print("\n" + "=" * 60)
    print("chart_state × flow_state 교차 (D+3 수익률)")
    print("=" * 60)
    if "d3" in df_results.columns:
        pivot = df_results.groupby(["chart_state", "flow_state"])["d3"].agg(["mean", "count"])
        pivot.columns = ["avg_d3", "count"]
        pivot = pivot.sort_values("avg_d3", ascending=False)
        print(f"\n{'chart_state':<12} {'flow_state':<15} {'건수':>6} {'D+3 평균':>10} ")
        print("-" * 50)
        for (cs, fs), row in pivot.iterrows():
            if row["count"] >= 5:  # 최소 5건 이상만
                print(f"  {cs:<12} {fs:<15} {int(row['count']):>6} {row['avg_d3']:>+9.2f}%")

    print("\n" + "=" * 60)
    print("신호별 D+3 수익률")
    print("=" * 60)
    if "d3" in df_results.columns:
        for sig in ["gc", "rsi_reclaim", "reversal", "gge"]:
            sub = df_results[df_results["signals"].str.contains(sig)]
            if len(sub) >= 5:
                avg = sub["d3"].mean()
                win = (sub["d3"] > 0).mean() * 100
                print(f"  {sig:<15} {len(sub):>5}건  D+3 {avg:+.2f}%  승률 {win:.1f}%")

    # 조합 분석
    print("\n" + "=" * 60)
    print("최적 조합 (D+3 기준, 10건 이상)")
    print("=" * 60)
    if "d3" in df_results.columns:
        # chart_state + flow_state + 신호 조합
        df_results["combo"] = df_results["chart_state"] + "/" + df_results["flow_state"]
        combo = df_results.groupby("combo")["d3"].agg(["mean", "count", lambda x: (x > 0).mean() * 100])
        combo.columns = ["avg", "count", "winrate"]
        combo = combo[combo["count"] >= 10].sort_values("avg", ascending=False)
        print(f"\n{'조합':<30} {'건수':>6} {'D+3 평균':>10} {'승률':>8}")
        print("-" * 58)
        for idx, row in combo.head(10).iterrows():
            print(f"  {idx:<30} {int(row['count']):>6} {row['avg']:>+9.2f}% {row['winrate']:>7.1f}%")

    # CSV 저장
    out_path = Path("tools/backtest_results.csv")
    out_path.parent.mkdir(exist_ok=True)
    df_results.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n상세 결과 저장: {out_path}")
    print(f"총 {len(df_results)}건, {df_results['code'].nunique()}종목")


if __name__ == "__main__":
    run_backtest()
