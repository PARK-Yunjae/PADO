"""ClosingBell 눌림목 TOP3 — 감시종목 중 진입 조건 충족 종목."""

import pandas as pd
from config import OHLCV_DIR, setup_logging

logger = setup_logging().getChild("cb_watchlist")


def check_pullbacks(watchlist: list[dict], api=None) -> list[dict]:
    """
    감시종목에서 눌림목 조건 충족한 TOP3 추출.
    
    눌림목 조건:
    - ma5 또는 ma8 터치 (현재가 대비 ±2%)
    - 거래량 급감 (MA20의 30% 이하)
    - RSI < 60
    """
    hits = []

    for item in watchlist:
        code = item.get("code", "")
        name = item.get("name", code)

        df = _load_ohlcv(code)
        if df is None or len(df) < 20:
            continue

        last = df.iloc[-1]
        ma5 = df["close"].rolling(5).mean().iloc[-1]
        ma8 = df["close"].rolling(8).mean().iloc[-1]
        ma33 = df["close"].rolling(33).mean().iloc[-1]
        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]

        # 현재가 (API 있으면 실시간, 없으면 종가)
        current = last["close"]
        if api:
            try:
                price_data = api.get_current_price(code)
                current = price_data.get("current_price", current)
            except Exception:
                pass

        # 눌림목 조건
        ma5_touch = abs(current - ma5) / ma5 < 0.02 if ma5 > 0 else False
        ma8_touch = abs(current - ma8) / ma8 < 0.02 if ma8 > 0 else False
        ma33_touch = abs(current - ma33) / ma33 < 0.02 if ma33 > 0 else False
        vol_dryup = last["volume"] < vol_ma20 * 0.30 if vol_ma20 > 0 else False

        rsi = _calc_rsi(df)

        if (ma5_touch or ma8_touch or ma33_touch) and vol_dryup and rsi < 60:
            support_line = ma33 if ma33_touch else (ma8 if ma8_touch else ma5)
            hits.append({
                "code": code, "name": name,
                "current_price": current,
                "support_line": round(support_line, 0),
                "vol_ratio_pct": round(last["volume"] / vol_ma20 * 100, 0) if vol_ma20 > 0 else 0,
                "rsi": round(rsi, 1),
                "score": item.get("score", 0),
                "note": item.get("note", ""),
                **item,  # 기존 watchlist 필드 유지
            })

    # 점수순 TOP3
    hits.sort(key=lambda x: x.get("score", 0), reverse=True)
    return hits[:3]


def _calc_rsi(df, period=14):
    import numpy as np
    delta = df["close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period).mean()
    rs = gain / loss.replace(0, float("nan"))
    rsi = 100 - 100 / (1 + rs)
    val = rsi.iloc[-1]
    return float(val) if pd.notna(val) else 50.0


def _load_ohlcv(code):
    p = OHLCV_DIR / f"{code}.csv"
    if not p.exists(): return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
        df.columns = [c.strip().lower() for c in df.columns]
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        for c in ("open","high","low","close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume",0), errors="coerce").fillna(0).astype(int)
        return df.dropna(subset=["close"])
    except: return None
