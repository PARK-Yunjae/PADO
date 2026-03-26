"""
하림지주/펄어비스 케이스 시뮬레이터
====================================
거래량 폭발 → 급감 눌림목 패턴 + 공매도/거래원 이상 체크

실행:
    cd C:\\Coding\\PADO
    python tools/case_sim.py --code 003380     # 하림지주
    python tools/case_sim.py --code 263750     # 펄어비스
    python tools/case_sim.py --code 003380 263750  # 둘 다
"""

import argparse
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import OHLCV_DIR, KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL


def load_ohlcv(code: str) -> pd.DataFrame | None:
    p = OHLCV_DIR / f"{code}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)
    for c in ("open", "high", "low", "close"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
    return df.dropna(subset=["close"])


def find_explosion_days(df: pd.DataFrame, multi: float = 3.0, lookback: int = 20) -> list[dict]:
    """거래량 폭발일 찾기 (MA20 대비 N배)."""
    ma20 = df["volume"].rolling(lookback).mean()
    explosions = []

    for i in range(lookback, len(df)):
        if ma20.iloc[i] <= 0:
            continue
        ratio = df.iloc[i]["volume"] / ma20.iloc[i]
        if ratio >= multi:
            explosions.append({
                "idx": i,
                "date": df.iloc[i]["date"],
                "close": df.iloc[i]["close"],
                "volume": df.iloc[i]["volume"],
                "vol_ma20": ma20.iloc[i],
                "ratio": round(ratio, 1),
                "change_pct": round((df.iloc[i]["close"] - df.iloc[i - 1]["close"]) / df.iloc[i - 1]["close"] * 100, 1),
                "is_bullish": df.iloc[i]["close"] >= df.iloc[i]["open"],
            })

    return explosions


def analyze_decline_after(df: pd.DataFrame, exp: dict, days: int = 5) -> list[dict]:
    """폭발일 이후 D+1~D+5 거래량 급감 추적 (유목민 기준)."""
    idx = exp["idx"]
    exp_vol = exp["volume"]
    results = []

    for d in range(1, days + 1):
        if idx + d >= len(df):
            break
        row = df.iloc[idx + d]
        vol_pct = round(row["volume"] / exp_vol * 100, 1) if exp_vol > 0 else 0
        change = round((row["close"] - df.iloc[idx + d - 1]["close"]) / df.iloc[idx + d - 1]["close"] * 100, 2)

        # 유목민 기준
        nomad_signal = ""
        if d == 2 and vol_pct <= 35:
            nomad_signal = "⚡ D+2 35% 이하 (유목민 1차)"
        elif d == 3 and vol_pct <= 20:
            nomad_signal = "⚡ D+3 20% 이하 (유목민 2차)"
        elif vol_pct <= 30:
            nomad_signal = "📉 30% 이하 급감"

        # 이평선 터치 확인
        ma5 = df["close"].rolling(5).mean().iloc[idx + d]
        ma8 = df["close"].rolling(8).mean().iloc[idx + d]
        touch = ""
        if abs(row["close"] - ma5) / ma5 < 0.02:
            touch = "MA5 터치"
        elif abs(row["close"] - ma8) / ma8 < 0.02:
            touch = "MA8 터치"

        results.append({
            "d_plus": d,
            "date": row["date"].strftime("%Y-%m-%d"),
            "close": row["close"],
            "volume": row["volume"],
            "vol_pct": vol_pct,
            "change_pct": change,
            "nomad_signal": nomad_signal,
            "ma_touch": touch,
            "is_bearish": row["close"] < row["open"],
        })

    return results


def calc_returns(df: pd.DataFrame, entry_idx: int, hold_days: list[int] = [1, 2, 3, 5]) -> dict:
    """진입일 기준 D+N 수익률."""
    entry_price = df.iloc[entry_idx]["close"]
    returns = {}
    for d in hold_days:
        if entry_idx + d < len(df):
            exit_price = df.iloc[entry_idx + d]["close"]
            returns[f"D+{d}"] = round((exit_price - entry_price) / entry_price * 100, 2)
    return returns


def check_supply_live(code: str) -> dict | None:
    """키움 API로 실시간 공매도/기관외인/거래원 조회."""
    if not KIWOOM_APPKEY:
        return None

    from shared.kiwoom_api import KiwoomAPI
    api = KiwoomAPI(KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL)

    result = {}

    # 공매도
    try:
        time.sleep(0.3)
        shorts = api.get_short_selling(code, days=5)
        if shorts:
            avg = sum(s.get("short_ratio", 0) for s in shorts) / len(shorts)
            result["short_ratio_5d"] = round(avg, 2)
            result["short_detail"] = shorts[:3]
    except Exception as e:
        result["short_error"] = str(e)

    # 기관/외인
    try:
        time.sleep(0.3)
        trends = api.get_investor_trend(code, days=5)
        if trends:
            foreign = sum(t.get("foreign", 0) for t in trends)
            inst = sum(t.get("institution", 0) for t in trends)
            result["foreign_5d_net"] = foreign
            result["institution_5d_net"] = inst
            result["investor_detail"] = trends[:3]
    except Exception as e:
        result["investor_error"] = str(e)

    # 거래원
    try:
        time.sleep(0.3)
        brokers = api.get_broker_ranking(code, period="5")
        if brokers:
            result["top_buyers"] = brokers[:3]
            result["top_sellers"] = brokers[-3:] if len(brokers) > 3 else []
    except Exception as e:
        result["broker_error"] = str(e)

    return result


def print_case_report(code: str, name: str = ""):
    print(f"\n{'='*60}")
    print(f"  {name} ({code}) 케이스 분석")
    print(f"{'='*60}")

    df = load_ohlcv(code)
    if df is None:
        print(f"  ❌ OHLCV 파일 없음: {OHLCV_DIR / f'{code}.csv'}")
        return

    print(f"  데이터: {df.iloc[0]['date'].date()} ~ {df.iloc[-1]['date'].date()} ({len(df)}일)")

    # 최근 60일 내 거래량 폭발일
    recent = df.tail(60).copy()
    recent_start_idx = len(df) - 60

    explosions = find_explosion_days(df, multi=2.5)
    recent_explosions = [e for e in explosions if e["idx"] >= recent_start_idx]

    if not recent_explosions:
        print(f"  최근 60일 내 거래량 폭발일 없음 (MA20 × 2.5배 기준)")
        # 기준 낮춰서 다시
        explosions = find_explosion_days(df, multi=2.0)
        recent_explosions = [e for e in explosions if e["idx"] >= recent_start_idx]
        if recent_explosions:
            print(f"  → 2.0배 기준으로 {len(recent_explosions)}건 발견")

    for exp in recent_explosions[-5:]:  # 최근 5건
        print(f"\n  📊 폭발일: {exp['date'].strftime('%Y-%m-%d')}")
        print(f"     종가 {exp['close']:,.0f} | 등락 {exp['change_pct']:+.1f}%")
        print(f"     거래량 {exp['volume']:,} (MA20의 {exp['ratio']}배)")
        print(f"     {'🟢 양봉' if exp['is_bullish'] else '🔴 음봉'}")

        # D+1~5 급감 추적
        declines = analyze_decline_after(df, exp, days=5)
        print(f"\n     {'D+':>4} {'날짜':>12} {'종가':>10} {'거래량잔존':>10} {'등락':>8} {'시그널'}")
        print(f"     {'─'*65}")

        best_entry = None
        for d in declines:
            marker = "  ←" if d["nomad_signal"] else ""
            touch = f" [{d['ma_touch']}]" if d["ma_touch"] else ""
            candle = "음봉" if d["is_bearish"] else "양봉"
            print(f"     D+{d['d_plus']}  {d['date']}  {d['close']:>9,.0f}  "
                  f"{d['vol_pct']:>8.1f}%  {d['change_pct']:>+7.2f}%  "
                  f"{d['nomad_signal']}{touch} ({candle})")

            if d["nomad_signal"] and not best_entry:
                best_entry = d

        # 시그널 발생 시 수익률
        if best_entry:
            entry_date = best_entry["date"]
            entry_idx = df[df["date"].dt.strftime("%Y-%m-%d") == entry_date].index[0]
            returns = calc_returns(df, entry_idx)
            print(f"\n     📈 시그널 진입 ({entry_date}) 기준 수익률:")
            for k, v in returns.items():
                icon = "🟢" if v > 0 else "🔴"
                print(f"        {icon} {k}: {v:+.2f}%")

    # 실시간 수급 (API 있으면)
    print(f"\n  {'─'*50}")
    print(f"  🔍 현재 수급 상태 (키움 API)")
    supply = check_supply_live(code)
    if supply is None:
        print(f"     API 키 미설정 — 스킵")
    else:
        if "short_ratio_5d" in supply:
            sr = supply["short_ratio_5d"]
            label = "🟢 낮음" if sr < 3 else ("🔴 높음" if sr > 5 else "🟡 보통")
            print(f"     공매도 5일 평균: {sr}% ({label})")
        if "foreign_5d_net" in supply:
            f = supply["foreign_5d_net"]
            i = supply["institution_5d_net"]
            print(f"     외인 5일 순매수: {f:+,}주")
            print(f"     기관 5일 순매수: {i:+,}주")
        if "top_buyers" in supply:
            print(f"     거래원 TOP3 매수:")
            for b in supply["top_buyers"][:3]:
                print(f"       {b.get('name', '?')}: {b.get('buy_amount', 0):,}")
        if "short_error" in supply:
            print(f"     공매도 조회 실패: {supply['short_error']}")


# 종목명 매핑 (하드코딩 — 로컬 매핑파일 못 읽을 때 대비)
KNOWN_NAMES = {
    "003380": "하림지주",
    "263750": "펄어비스",
    "005930": "삼성전자",
    "000660": "SK하이닉스",
}


def main():
    parser = argparse.ArgumentParser(description="케이스 시뮬 — 거래량 폭발 후 급감 패턴 분석")
    parser.add_argument("--code", nargs="+", default=["003380", "263750"],
                        help="종목코드 (기본: 하림지주 + 펄어비스)")
    args = parser.parse_args()

    # 종목명 매핑 시도
    try:
        from shared.stock_map import load_stock_map
        smap = load_stock_map()
    except:
        smap = {}

    for code in args.code:
        code = code.zfill(6)
        info = smap.get(code)
        name = info.name if info else KNOWN_NAMES.get(code, code)
        print_case_report(code, name)

    print(f"\n{'='*60}")
    print(f"  분석 완료")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
