"""
눌림목 시그널 성과 추적
========================
pullback_signals 테이블의 D+1~D+5 수익률을 자동 업데이트합니다.
매일 파이프라인 후에 실행하면 됩니다.

실행:
    python tools/pullback_tracker.py           # 최근 7일 시그널 추적
    python tools/pullback_tracker.py --days 30 # 최근 30일
    python tools/pullback_tracker.py --report  # 승률 리포트
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import OHLCV_DIR, setup_logging
from shared import storage

logger = setup_logging().getChild("pb_tracker")


def update_returns(days: int = 7):
    """pullback_signals의 D+N 수익률 업데이트."""
    signals = storage.get_pullback_signals(days=days)
    if not signals:
        print(f"최근 {days}일간 시그널 없음")
        return

    updated = 0
    for sig in signals:
        code = sig["code"]
        signal_date = sig["signal_date"]

        # 이미 D+5까지 다 채워져 있으면 스킵
        if all(sig.get(f"d{d}_return") is not None for d in [1, 2, 3, 5]):
            continue

        p = OHLCV_DIR / f"{code}.csv"
        if not p.exists():
            continue

        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            df.columns = [c.strip().lower() for c in df.columns]
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)

            # 시그널 날짜 찾기
            sig_dt = pd.to_datetime(signal_date)
            sig_rows = df[df["date"] == sig_dt]
            if sig_rows.empty:
                continue
            sig_idx = sig_rows.index[0]

            # 다음날 시가 = 진입가
            entry_idx = sig_idx + 1
            if entry_idx >= len(df):
                continue
            entry_price = float(df.iloc[entry_idx]["open"])
            if entry_price <= 0:
                continue

            # D+N 수익률
            returns = {}
            for hold in [1, 2, 3, 5]:
                exit_idx = entry_idx + hold
                if exit_idx < len(df):
                    exit_price = float(df.iloc[exit_idx]["close"])
                    returns[f"d{hold}"] = round((exit_price - entry_price) / entry_price * 100, 2)

            if returns:
                storage.update_pullback_returns(
                    code, signal_date,
                    d1=returns.get("d1"),
                    d2=returns.get("d2"),
                    d3=returns.get("d3"),
                    d5=returns.get("d5"),
                )
                updated += 1

        except Exception as e:
            logger.debug(f"수익률 업데이트 실패 {code}: {e}")

    print(f"수익률 업데이트: {updated}건 (대상 {len(signals)}건)")


def print_report(days: int = 30):
    """승률 리포트."""
    signals = storage.get_pullback_signals(days=days)
    if not signals:
        print(f"최근 {days}일간 시그널 없음")
        return

    df = pd.DataFrame(signals)
    print(f"\n{'='*60}")
    print(f"  눌림목 시그널 성과 리포트 (최근 {days}일)")
    print(f"  총 시그널: {len(df)}건")
    print(f"{'='*60}")

    # 판정별 통계
    print(f"\n📊 판정별 D+3 승률")
    print(f"{'─'*50}")
    for verdict in ["PASS", "WARN", "REJECT"]:
        sub = df[df["verdict"] == verdict]
        d3 = pd.to_numeric(sub["d3_return"], errors="coerce").dropna()
        if len(d3) == 0:
            print(f"  {verdict:<8} {len(sub):>4}건  (수익률 미집계)")
            continue
        wr = (d3 > 0).sum() / len(d3) * 100
        print(f"  {verdict:<8} {len(sub):>4}건  승률 {wr:>5.1f}%  평균 {d3.mean():>+.2f}%")

    # D+N별
    print(f"\n📊 D+N별 승률 (PASS만)")
    print(f"{'─'*50}")
    passes = df[df["verdict"] == "PASS"]
    for col in ["d1_return", "d2_return", "d3_return", "d5_return"]:
        vals = pd.to_numeric(passes[col], errors="coerce").dropna()
        if len(vals) == 0:
            continue
        label = col.replace("_return", "").upper().replace("D", "D+")
        wr = (vals > 0).sum() / len(vals) * 100
        print(f"  {label:<6} {len(vals):>4}건  승률 {wr:>5.1f}%  평균 {vals.mean():>+.2f}%")

    # 개별 종목
    print(f"\n📊 최근 시그널 상세")
    print(f"{'─'*60}")
    recent = df.tail(10)
    for _, r in recent.iterrows():
        icon = {"PASS": "📌", "WARN": "👀", "REJECT": "❌"}.get(r.get("verdict", ""), "?")
        d3 = r.get("d3_return", "")
        d3_str = f"D+3 {d3:+.1f}%" if pd.notna(d3) and d3 != "" else "D+3 미집계"
        print(f"  {icon} {r['signal_date']} {r['name']:<12} D+{r['d_plus']} "
              f"잔존{r['vol_remain_pct']:.0f}% → {d3_str}")


def main():
    parser = argparse.ArgumentParser(description="눌림목 시그널 성과 추적")
    parser.add_argument("--days", type=int, default=7, help="추적 기간 (기본 7일)")
    parser.add_argument("--report", action="store_true", help="승률 리포트 출력")
    args = parser.parse_args()

    if args.report:
        print_report(days=args.days)
    else:
        update_returns(days=args.days)
        print_report(days=args.days)


if __name__ == "__main__":
    main()
