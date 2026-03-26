"""
전종목 눌림목 백테스트 (풀스캔)
================================
2016년 6월 ~ 현재, 전종목 OHLCV 스캔.
유목민 거래량법 기준 폭발→급감→수익률 계산.

분석 항목:
  시간: 연도별, 분기별, 월별, 주간별
  분류: D+N별, 시그널타입별, 강도별, 음봉/양봉, 이평선터치,
        20일선 위/아래, 가격대별, 섹터별, 복합조건

실행:
    python tools/pullback_backtest.py                       # 전체 (2016.06~)
    python tools/pullback_backtest.py --start 2024-01-01    # 2024년부터
    python tools/pullback_backtest.py --start 2020-01-01 --end 2023-12-31
    python tools/pullback_backtest.py --output results      # CSV 저장
    python tools/pullback_backtest.py --codes 003380 263750 # 특정 종목만
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import OHLCV_DIR, setup_logging

logger = setup_logging().getChild("pullback_bt")

# ── 유목민 기준 ──
EXPLOSION_MULTI = 3.0
MAX_WATCH_DAYS = 5
D2_THRESHOLD = 0.35
D3_THRESHOLD = 0.20
GENERIC_THRESHOLD = 0.30
BEST_THRESHOLD = 0.12
MIN_PRICE = 2000
MAX_PRICE = 150000


# ═══════════════════════════════════════════════
# 데이터 로드 + 시그널 스캔
# ═══════════════════════════════════════════════

def load_ohlcv(code: str) -> pd.DataFrame | None:
    p = OHLCV_DIR / f"{code}.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig", on_bad_lines="skip")
        df.columns = [c.strip().lower() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        for c in ("open", "high", "low", "close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
        return df.dropna(subset=["close"])
    except Exception:
        return None


def scan_one(df: pd.DataFrame, start_date: str, end_date: str) -> list[dict]:
    """한 종목에서 폭발→급감 시그널 추출."""
    if len(df) < 30:
        return []

    ma20 = df["volume"].rolling(20).mean()
    ma5 = df["close"].rolling(5).mean()
    ma8 = df["close"].rolling(8).mean()
    ma20p = df["close"].rolling(20).mean()

    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date) if end_date else df["date"].max()

    signals = []

    for i in range(25, len(df) - MAX_WATCH_DAYS - 6):
        dt = df.iloc[i]["date"]
        if pd.isna(dt) or dt < start_dt or dt > end_dt:
            continue
        if pd.isna(ma20.iloc[i]) or ma20.iloc[i] <= 0:
            continue

        ratio = df.iloc[i]["volume"] / ma20.iloc[i]
        if ratio < EXPLOSION_MULTI:
            continue

        price_exp = df.iloc[i]["close"]
        if price_exp < MIN_PRICE or price_exp > MAX_PRICE:
            continue

        exp_vol = int(df.iloc[i]["volume"])

        for d in range(1, MAX_WATCH_DAYS + 1):
            idx = i + d
            if idx >= len(df) - 5:
                break

            row = df.iloc[idx]
            vol_remain = row["volume"] / exp_vol * 100 if exp_vol > 0 else 100

            sig_type = ""
            strength = 0

            if d == 2 and vol_remain <= D2_THRESHOLD * 100:
                sig_type = "D+2_35%"
                strength = 2
            elif d == 3 and vol_remain <= D3_THRESHOLD * 100:
                sig_type = "D+3_20%"
                strength = 3
            elif vol_remain <= BEST_THRESHOLD * 100:
                sig_type = "극강_12%"
                strength = 4
            elif vol_remain <= GENERIC_THRESHOLD * 100:
                sig_type = "일반_30%"
                strength = 1
            else:
                continue

            is_bearish = row["close"] < row["open"]
            if is_bearish:
                strength += 1

            m5t = bool(ma5.iloc[idx] > 0 and abs(row["close"] - ma5.iloc[idx]) / ma5.iloc[idx] < 0.02)
            m8t = bool(ma8.iloc[idx] > 0 and abs(row["close"] - ma8.iloc[idx]) / ma8.iloc[idx] < 0.02)
            above20 = bool(pd.notna(ma20p.iloc[idx]) and row["close"] > ma20p.iloc[idx])

            if m5t or m8t:
                strength += 1

            # 수익률: 시그널 다음날 시가 매수 → N일 후 종가 청산
            entry_idx = idx + 1
            if entry_idx >= len(df):
                continue
            entry_price = df.iloc[entry_idx]["open"]
            if entry_price <= 0:
                continue

            rets = {}
            for h in [1, 2, 3, 5]:
                ex = entry_idx + h
                if ex < len(df):
                    rets[f"d{h}"] = round((df.iloc[ex]["close"] - entry_price) / entry_price * 100, 2)

            if not rets:
                continue

            sig_date = row["date"]
            signals.append({
                "signal_date": sig_date,
                "year": sig_date.year,
                "quarter": f"{sig_date.year}Q{(sig_date.month-1)//3+1}",
                "month": sig_date.strftime("%Y-%m"),
                "week": sig_date.strftime("%Y-W%W"),
                "d_plus": d,
                "exp_ratio": round(ratio, 1),
                "vol_remain": round(vol_remain, 1),
                "signal_type": sig_type,
                "strength": strength,
                "is_bearish": is_bearish,
                "ma5_touch": m5t,
                "ma8_touch": m8t,
                "above_ma20": above20,
                "price": float(row["close"]),
                **rets,
            })

    return signals


# ═══════════════════════════════════════════════
# 승률 분석 함수들
# ═══════════════════════════════════════════════

def _wr(series):
    """승률+평균+중앙값 계산."""
    v = series.dropna()
    if len(v) == 0:
        return None
    return {
        "n": len(v),
        "wr": round((v > 0).sum() / len(v) * 100, 1),
        "avg": round(v.mean(), 2),
        "med": round(v.median(), 2),
        "max": round(v.max(), 2),
        "min": round(v.min(), 2),
    }


def _print_header(title):
    print(f"\n📊 {title}")
    print(f"{'─'*70}")
    print(f"  {'분류':<25} {'시그널':>7} {'승률':>7} {'평균':>8} {'중앙값':>7} {'최대':>7} {'최소':>7}")
    print(f"{'─'*70}")


def _print_row(label, stats):
    if stats is None or stats["n"] < 5:
        return
    print(f"  {label:<25} {stats['n']:>7,} {stats['wr']:>6.1f}% "
          f"{stats['avg']:>+7.2f}% {stats['med']:>+6.2f}% "
          f"{stats['max']:>+6.1f}% {stats['min']:>+6.1f}%")


def analyze(df: pd.DataFrame, target="d3"):
    """모든 기준별 승률 분석."""
    print(f"\n{'='*70}")
    print(f"  전종목 눌림목 백테스트 결과")
    print(f"  총 시그널: {len(df):,}건 | 기간: {df['year'].min()}~{df['year'].max()}")
    print(f"  수익률 기준: 시그널 다음날 시가 매수 → 종가 청산")
    print(f"{'='*70}")

    # ── 1) D+N별 전체 ──
    _print_header("D+N별 전체 승률")
    for d in ["d1", "d2", "d3", "d5"]:
        _print_row(d.upper().replace("D", "D+"), _wr(df[d]))

    # ── 2) 연도별 ──
    _print_header(f"연도별 {target.upper()} 승률")
    for yr in sorted(df["year"].unique()):
        _print_row(str(yr), _wr(df[df["year"] == yr][target]))

    # ── 3) 분기별 ──
    _print_header(f"분기별 {target.upper()} 승률 (최근 8분기)")
    quarters = sorted(df["quarter"].unique())[-8:]
    for q in quarters:
        _print_row(q, _wr(df[df["quarter"] == q][target]))

    # ── 4) 월별 (1~12월 합산) ──
    _print_header(f"월별 {target.upper()} 승률 (전체 기간 합산)")
    df["_m"] = df["signal_date"].dt.month
    for m in range(1, 13):
        _print_row(f"{m}월", _wr(df[df["_m"] == m][target]))

    # ── 5) 주간별 (최근 12주) ──
    _print_header(f"주간별 {target.upper()} 승률 (최근 12주)")
    weeks = sorted(df["week"].unique())[-12:]
    for w in weeks:
        _print_row(w, _wr(df[df["week"] == w][target]))

    # ── 6) 시그널 타입별 ──
    _print_header(f"시그널 타입별 {target.upper()} 승률")
    for st in sorted(df["signal_type"].unique()):
        _print_row(st, _wr(df[df["signal_type"] == st][target]))

    # ── 7) 강도별 ──
    _print_header(f"시그널 강도별 {target.upper()} 승률")
    for s in sorted(df["strength"].unique()):
        _print_row(f"강도 {s}", _wr(df[df["strength"] == s][target]))

    # ── 8) 음봉/양봉 ──
    _print_header(f"음봉 vs 양봉 {target.upper()} 승률")
    _print_row("음봉", _wr(df[df["is_bearish"] == True][target]))
    _print_row("양봉", _wr(df[df["is_bearish"] == False][target]))

    # ── 9) 이평선 터치 ──
    _print_header(f"이평선 터치별 {target.upper()} 승률")
    _print_row("5일선 터치", _wr(df[df["ma5_touch"] == True][target]))
    _print_row("8일선 터치", _wr(df[df["ma8_touch"] == True][target]))
    _print_row("터치 없음", _wr(df[(df["ma5_touch"] == False) & (df["ma8_touch"] == False)][target]))

    # ── 10) 20일선 위/아래 ──
    _print_header(f"20일선 위치별 {target.upper()} 승률")
    _print_row("20일선 위", _wr(df[df["above_ma20"] == True][target]))
    _print_row("20일선 아래", _wr(df[df["above_ma20"] == False][target]))

    # ── 11) 가격대별 ──
    _print_header(f"가격대별 {target.upper()} 승률")
    bins = [(2000, 5000, "2천~5천"), (5000, 10000, "5천~1만"),
            (10000, 30000, "1만~3만"), (30000, 50000, "3만~5만"),
            (50000, 150000, "5만~15만")]
    for lo, hi, label in bins:
        _print_row(label, _wr(df[(df["price"] >= lo) & (df["price"] < hi)][target]))

    # ── 12) D+N별 (폭발일 기준) ──
    _print_header(f"폭발 후 D+N별 {target.upper()} 승률")
    for d in range(1, 6):
        _print_row(f"D+{d}", _wr(df[df["d_plus"] == d][target]))

    # ── 13) 폭발 배율별 ──
    _print_header(f"폭발 배율별 {target.upper()} 승률")
    ratio_bins = [(3.0, 5.0, "3~5배"), (5.0, 8.0, "5~8배"),
                  (8.0, 15.0, "8~15배"), (15.0, 999, "15배+")]
    for lo, hi, label in ratio_bins:
        _print_row(label, _wr(df[(df["exp_ratio"] >= lo) & (df["exp_ratio"] < hi)][target]))

    # ── 14) 잔존률 구간별 ──
    _print_header(f"잔존률 구간별 {target.upper()} 승률")
    vr_bins = [(0, 5, "0~5%"), (5, 12, "5~12%"), (12, 20, "12~20%"),
               (20, 30, "20~30%"), (30, 35, "30~35%")]
    for lo, hi, label in vr_bins:
        _print_row(label, _wr(df[(df["vol_remain"] >= lo) & (df["vol_remain"] < hi)][target]))

    # ── 15) 섹터별 ──
    if "sector" in df.columns:
        _print_header(f"섹터별 {target.upper()} 승률 (상위 20)")
        sector_stats = []
        for sec in df["sector"].unique():
            if not sec or sec == "기타":
                continue
            s = _wr(df[df["sector"] == sec][target])
            if s and s["n"] >= 10:
                sector_stats.append((sec, s))
        sector_stats.sort(key=lambda x: -x[1]["wr"])
        for sec, s in sector_stats[:20]:
            _print_row(sec, s)

    # ── 16) 복합 조건 ──
    _print_header(f"복합 조건 {target.upper()} 승률")
    combos = [
        ("음봉 + 이평터치",
         (df["is_bearish"]) & (df["ma5_touch"] | df["ma8_touch"])),
        ("음봉 + 20일선위",
         (df["is_bearish"]) & (df["above_ma20"])),
        ("강도3+ + 음봉",
         (df["strength"] >= 3) & (df["is_bearish"])),
        ("강도3+ + 20일선위",
         (df["strength"] >= 3) & (df["above_ma20"])),
        ("극강12% + 음봉",
         (df["signal_type"] == "극강_12%") & (df["is_bearish"])),
        ("D+2_35% + 음봉",
         (df["signal_type"] == "D+2_35%") & (df["is_bearish"])),
        ("D+3_20% + 음봉",
         (df["signal_type"] == "D+3_20%") & (df["is_bearish"])),
        ("음봉 + 이평 + 20일선위",
         (df["is_bearish"]) & (df["ma5_touch"] | df["ma8_touch"]) & (df["above_ma20"])),
        ("강도4+ (최강)",
         df["strength"] >= 4),
        ("D+2 + 잔존20%미만 + 음봉",
         (df["d_plus"] == 2) & (df["vol_remain"] < 20) & (df["is_bearish"])),
        ("폭발5배+ + 잔존20%미만",
         (df["exp_ratio"] >= 5) & (df["vol_remain"] < 20)),
    ]
    for label, mask in combos:
        _print_row(label, _wr(df.loc[mask, target]))

    # ── 17) 연도별 × 시그널타입 교차 ──
    _print_header(f"연도 × 시그널타입 {target.upper()} 승률 (최근 3년)")
    recent_years = sorted(df["year"].unique())[-3:]
    for yr in recent_years:
        for st in sorted(df["signal_type"].unique()):
            sub = df[(df["year"] == yr) & (df["signal_type"] == st)]
            s = _wr(sub[target])
            if s and s["n"] >= 5:
                _print_row(f"{yr} × {st}", s)


# ═══════════════════════════════════════════════
# 메인
# ═══════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="전종목 눌림목 백테스트")
    parser.add_argument("--start", type=str, default="2016-06-01", help="시작일 (기본 2016-06-01)")
    parser.add_argument("--end", type=str, default="", help="종료일 (기본 최근)")
    parser.add_argument("--output", type=str, default="", help="CSV 저장 폴더")
    parser.add_argument("--codes", nargs="+", default=[], help="특정 종목만")
    parser.add_argument("--target", type=str, default="d3", help="승률 기준 (d1/d2/d3/d5, 기본 d3)")
    args = parser.parse_args()

    end_date = args.end or datetime.now().strftime("%Y-%m-%d")
    print(f"스캔 기간: {args.start} ~ {end_date}")
    print(f"승률 기준: 시그널 다음날 시가 매수 → {args.target.upper()} 종가 청산")

    # 종목 매핑
    try:
        from shared.stock_map import load_stock_map
        smap = load_stock_map()
    except Exception:
        smap = {}

    # 종목 리스트
    if args.codes:
        codes = [c.zfill(6) for c in args.codes]
    else:
        codes = sorted([p.stem for p in OHLCV_DIR.glob("*.csv")])

    print(f"대상: {len(codes)}종목\n")

    # 제외 키워드 — 강화
    EXCLUDE_KW = [
        "ETF", "ETN", "KODEX", "TIGER", "KBSTAR", "ARIRANG", "HANARO", "SOL",
        "스팩", "SPAC", "리츠",
        "호스팩", "기업인수목적", "인버스", "레버리지",
    ]

    all_signals = []
    scanned = 0

    for code in codes:
        try:
            df = load_ohlcv(code)
        except Exception:
            continue
        if df is None or len(df) < 50:
            continue

        info = smap.get(code)
        name = info.name if info else code

        # ETF/스팩/우선주 제외
        if any(kw in name for kw in EXCLUDE_KW):
            continue
        if name.endswith(("우", "우B", "우C", "우(전환)", "1우", "2우", "3우")):
            continue

        signals = scan_one(df, args.start, args.end)

        sector = getattr(info, "sector", "기타") if info else "기타"

        for s in signals:
            s["code"] = code
            s["name"] = name
            s["sector"] = sector

        all_signals.extend(signals)
        scanned += 1

        if scanned % 500 == 0:
            print(f"  ... {scanned}종목 스캔, {len(all_signals):,}건 시그널")

    print(f"\n스캔 완료: {scanned}종목, {len(all_signals):,}건 시그널")

    if not all_signals:
        print("시그널 0건 — 종료")
        return

    df_all = pd.DataFrame(all_signals)

    # 승률 분석
    analyze(df_all, target=args.target)

    # CSV 저장
    if args.output:
        out = Path(args.output)
        out.mkdir(exist_ok=True)

        # 시그널 전체
        p1 = out / f"pullback_signals_{args.start}_{end_date}.csv"
        df_all.to_csv(p1, index=False, encoding="utf-8-sig")
        print(f"\n💾 시그널: {p1} ({len(df_all):,}건)")

        # 요약 통계
        rows = []
        for d in ["d1", "d2", "d3", "d5"]:
            s = _wr(df_all[d])
            if s:
                rows.append({"기간": d.upper(), **s})
        if rows:
            p2 = out / f"pullback_summary_{args.start}_{end_date}.csv"
            pd.DataFrame(rows).to_csv(p2, index=False, encoding="utf-8-sig")
            print(f"💾 요약: {p2}")

        # 연도별 CSV
        yr_rows = []
        for yr in sorted(df_all["year"].unique()):
            for d in ["d1", "d2", "d3", "d5"]:
                s = _wr(df_all[df_all["year"] == yr][d])
                if s:
                    yr_rows.append({"연도": yr, "기간": d.upper(), **s})
        if yr_rows:
            p3 = out / f"pullback_yearly_{args.start}_{end_date}.csv"
            pd.DataFrame(yr_rows).to_csv(p3, index=False, encoding="utf-8-sig")
            print(f"💾 연도별: {p3}")

    print(f"\n{'='*70}")
    print(f"  백테스트 완료")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
