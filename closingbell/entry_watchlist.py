"""
ClosingBell 눌림목 — 유목민 거래량법 기준
==========================================
폭발일 거래량 대비 잔존률로 판정.
D+2 35%이하=1차, D+3 20%이하=2차(최강), D+5 초과=해제.
"""

import pandas as pd
import numpy as np
from config import OHLCV_DIR, setup_logging

logger = setup_logging().getChild("cb_watchlist")

EXPLOSION_MULTI = 2.5
MAX_WATCH_DAYS = 5
D2_THRESHOLD = 0.35
D3_THRESHOLD = 0.20
GENERIC_THRESHOLD = 0.30
BEST_THRESHOLD = 0.12


def check_pullbacks(watchlist: list[dict], api=None) -> list[dict]:
    """감시종목에서 유목민 기준 눌림목 충족 종목 추출."""
    hits = []
    seen_codes = set()

    for item in watchlist:
        code = item.get("code", "")
        if code in seen_codes:
            continue
        name = item.get("name", code)

        df = _load_ohlcv(code)
        if df is None or len(df) < 25:
            continue

        last = df.iloc[-1]
        current = float(last["close"])
        intraday_volume = int(last["volume"])

        if api:
            try:
                price_data = api.get_current_price(code)
                current = float(price_data.get("price", current))
                intraday_volume = int(price_data.get("volume", intraday_volume))
            except Exception:
                pass

        # 최근 폭발일 찾기
        ma20 = df["volume"].rolling(20).mean()
        explosion = None
        search_start = max(0, len(df) - MAX_WATCH_DAYS - 1)
        best_ratio = 0

        for i in range(len(df) - 1, search_start, -1):
            if pd.isna(ma20.iloc[i]) or ma20.iloc[i] <= 0:
                continue
            ratio = df.iloc[i]["volume"] / ma20.iloc[i]
            if ratio >= EXPLOSION_MULTI and ratio > best_ratio:
                best_ratio = ratio
                explosion = {
                    "idx": i,
                    "date": df.iloc[i]["date"],
                    "volume": int(df.iloc[i]["volume"]),
                    "close": float(df.iloc[i]["close"]),
                    "ratio": round(ratio, 1),
                }

        if explosion is None:
            continue

        d_plus = len(df) - 1 - explosion["idx"]
        if d_plus < 1 or d_plus > MAX_WATCH_DAYS:
            continue

        exp_vol = explosion["volume"]
        vol_remain = intraday_volume / exp_vol * 100 if exp_vol > 0 else 100

        # 유목민 시그널 판정
        signal = ""
        strength = 0

        if d_plus == 2 and vol_remain <= D2_THRESHOLD * 100:
            signal = f"⚡ D+2 {vol_remain:.0f}% (1차)"
            strength = 2
        elif d_plus == 3 and vol_remain <= D3_THRESHOLD * 100:
            signal = f"⚡ D+3 {vol_remain:.0f}% (2차·최강)"
            strength = 3
        elif vol_remain <= BEST_THRESHOLD * 100:
            signal = f"🔥 잔존{vol_remain:.0f}% (극강)"
            strength = 4
        elif vol_remain <= GENERIC_THRESHOLD * 100:
            signal = f"📉 잔존{vol_remain:.0f}%"
            strength = 1
        else:
            continue

        is_bearish = current < float(last.get("open", current))
        if is_bearish:
            strength += 1

        ma5 = df["close"].rolling(5).mean().iloc[-1]
        ma8 = df["close"].rolling(8).mean().iloc[-1]
        ma_touch = ""
        if ma5 > 0 and abs(current - ma5) / ma5 < 0.02:
            ma_touch = "5일선"
            strength += 1
        elif ma8 > 0 and abs(current - ma8) / ma8 < 0.02:
            ma_touch = "8일선"
            strength += 1

        support = ma8 if ma_touch == "8일선" else (ma5 if ma_touch == "5일선" else min(ma5, ma8))
        pb_entry = round(support * 1.005, 0)
        pb_stop = round(support * 0.97, 0)
        high_20d = df["high"].tail(20).max()
        pb_target = round(min(explosion["close"], high_20d) * 0.98, 0)
        if pb_target <= pb_entry:
            pb_target = round(current * 1.05, 0)

        df_live = df.copy()
        df_live.at[df_live.index[-1], "close"] = current
        df_live.at[df_live.index[-1], "volume"] = intraday_volume
        rsi = _calc_rsi(df_live)

        exp_date = explosion["date"]
        exp_date_str = exp_date.strftime("%m/%d") if hasattr(exp_date, "strftime") else str(exp_date)[-5:]

        seen_codes.add(code)
        hits.append({
            "code": code, "name": name,
            "current_price": current,
            "support_line": round(support, 0),
            "vol_ratio_pct": round(vol_remain, 0),
            "rsi": round(rsi, 1),
            "entry_price": pb_entry,
            "stop_loss": pb_stop,
            "target_price": pb_target,
            "grade": item.get("grade", ""),
            "score": item.get("score", 0),
            "signal": signal,
            "signal_strength": strength,
            "d_plus": d_plus,
            "explosion_date": exp_date_str,
            "explosion_ratio": explosion["ratio"],
            "ma_touch": ma_touch,
            "note": (f"폭발({explosion['ratio']}배) D+{d_plus} "
                     f"잔존{vol_remain:.0f}%"
                     + (f" {ma_touch}터치" if ma_touch else "")
                     + (" 음봉" if is_bearish else "")),
        })

    hits.sort(key=lambda x: (-x["signal_strength"], x["d_plus"]))
    return hits[:5]


def _calc_rsi(df, period=14):
    delta = df["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period).mean()
    rs = gain / loss.replace(0, float("nan"))
    rsi = 100 - 100 / (1 + rs)
    val = rsi.iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def _load_ohlcv(code):
    p = OHLCV_DIR / f"{code}.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
        df.columns = [c.strip().lower() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        for c in ("open", "high", "low", "close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
        return df.dropna(subset=["close"])
    except Exception:
        return None
