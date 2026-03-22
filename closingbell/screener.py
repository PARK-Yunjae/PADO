"""
ClosingBell 스크리너 — 순수 차트 100점
========================================
형님용. DART/뉴스/AI 없음.
v3: 거래량상위 + 거래대금상위 합집합 유니버스 (기존 CB 복원).
"""

import pandas as pd
from config import (
    OHLCV_DIR, CB_SCORE_RSI, CB_SCORE_MA_ALIGN, CB_SCORE_CHANGE,
    CB_SCORE_VOL_BURST, CB_SCORE_BROKER, CB_SCORE_SHORT,
    CB_SCORE_GC_BONUS, CB_SCORE_GGE_BONUS, CB_SCORE_OBV_BONUS,
    CB_OVERHEAT_RSI,
    CB_UNIVERSE_TOP_N, CB_MIN_PRICE, CB_MAX_PRICE, CB_ETF_KEYWORDS,
    setup_logging,
)
from shared.stock_map import load_stock_map
from shared import storage

logger = setup_logging().getChild("cb_screener")


class CBScreener:

    def __init__(self, api=None):
        self.api = api
        self.stock_map = load_stock_map()

    def run(self, date: str = "") -> dict:
        """유니버스 → 필터 → 100점 스코어링 → TOP5 → DB 저장."""
        universe = self._get_universe()
        if not universe:
            logger.warning("유니버스 비어있음")
            return {"date": date, "stocks": []}

        scored = []
        for stock in universe:
            try:
                result = self._score(stock)
                if result:
                    scored.append(result)
            except Exception as e:
                logger.debug(f"CB 스코어링 실패 {stock.get('code')}: {e}")

        scored.sort(key=lambda x: x["score"], reverse=True)
        top5 = scored[:5]

        payload = {"date": date, "stocks": top5, "universe_size": len(universe),
                   "scored_count": len(scored)}
        storage.save_cb_screen(date, payload)
        logger.info(f"CB 스크리닝 완료: {len(scored)}종목 (유니버스 {len(universe)}), TOP5 저장")
        return payload

    def _get_universe(self) -> list[dict]:
        """거래량상위 + 거래대금상위 합집합.

        기존 ClosingBell과 동일한 방식:
        - ka10030: 거래량상위 TOP N (거래대금 100억+)
        - ka10032: 거래대금상위 TOP N
        - 합집합 + core(교집합)/fringe(나머지) 태깅
        - 가격 1,000~150,000원
        - ETF/스팩/우선주 제외
        """
        if not self.api:
            return []

        top_n = CB_UNIVERSE_TOP_N

        # ka10030: 거래량상위
        vol_stocks = {}
        try:
            vol_rank = self.api.get_volume_rank(market="000", min_trading_value="1000")
            for s in vol_rank[:top_n]:
                vol_stocks[s["code"]] = s
        except Exception as e:
            logger.warning(f"ka10030 거래량상위 실패: {e}")

        # ka10032: 거래대금상위
        val_stocks = {}
        try:
            val_rank = self.api.get_trading_value_rank(market="000")
            for s in val_rank[:top_n]:
                val_stocks[s["code"]] = s
        except Exception as e:
            logger.warning(f"ka10032 거래대금상위 실패: {e}")

        # 합집합 + core/fringe 태깅
        vol_codes = set(vol_stocks.keys())
        val_codes = set(val_stocks.keys())
        core_codes = vol_codes & val_codes

        seen = {}
        for code, s in vol_stocks.items():
            s["pool_type"] = "core" if code in core_codes else "fringe"
            seen[code] = s
        for code, s in val_stocks.items():
            if code not in seen:
                s["pool_type"] = "fringe"
                seen[code] = s

        universe = list(seen.values())

        # 필터링
        filtered = []
        for s in universe:
            name = s.get("name", "")
            price = s.get("price", 0)

            # 가격 필터
            if price < CB_MIN_PRICE or price > CB_MAX_PRICE:
                continue

            # ETF/스팩 키워드 필터
            if any(kw in name for kw in CB_ETF_KEYWORDS):
                continue

            # 우선주 필터 (이름 끝 "우", "우B", "우C")
            if name.endswith("우") or name.endswith("우B") or name.endswith("우C"):
                continue

            # stock_map에서 이름/섹터 보완
            info = self.stock_map.get(s["code"])
            if info and not name:
                s["name"] = info.name
            if info:
                s["sector"] = info.sector

            filtered.append(s)

        core_count = sum(1 for s in filtered if s.get("pool_type") == "core")
        logger.info(
            f"CB 유니버스: Core {core_count} / Fringe {len(filtered)-core_count} "
            f"= 총 {len(filtered)}종목 (거래량 {len(vol_codes)}, 거래대금 {len(val_codes)}, "
            f"필터 전 {len(universe)})"
        )

        return filtered

    def _score(self, stock: dict) -> dict | None:
        """단일 종목 100점 스코어링."""
        code = str(stock.get("code", "")).zfill(6)
        df = self._load_ohlcv(code)
        if df is None or len(df) < 33:
            return None

        score = 0.0
        reasons = []
        last = df.iloc[-1]

        # RSI (CB_SCORE_RSI)
        rsi = self._calc_rsi(df)
        if 50 <= rsi <= 60:
            score += CB_SCORE_RSI; reasons.append(f"RSI {rsi:.0f}")
        elif 40 <= rsi < 50 or 60 < rsi <= 70:
            score += CB_SCORE_RSI * 0.5

        # 이평 배열 (CB_SCORE_MA_ALIGN)
        ma8 = df["close"].rolling(8).mean().iloc[-1]
        ma33 = df["close"].rolling(33).mean().iloc[-1]
        if ma8 > ma33:
            score += CB_SCORE_MA_ALIGN; reasons.append("정배열")
        elif abs(ma8 - ma33) / ma33 < 0.02:
            score += CB_SCORE_MA_ALIGN * 0.5

        # 등락률 (CB_SCORE_CHANGE)
        if len(df) >= 2:
            change = (last["close"] - df.iloc[-2]["close"]) / df.iloc[-2]["close"] * 100
            if 3 <= change <= 6:
                score += CB_SCORE_CHANGE; reasons.append(f"등락 {change:.1f}%")
            elif 1 <= change < 3 or 6 < change <= 10:
                score += CB_SCORE_CHANGE * 0.5

        # 거래량 폭발 (CB_SCORE_VOL_BURST)
        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]
        if vol_ma20 > 0:
            vol_ratio = last["volume"] / vol_ma20
            if 2 <= vol_ratio <= 8:
                score += CB_SCORE_VOL_BURST; reasons.append(f"거래량 {vol_ratio:.1f}배")

        # GC 보너스
        gc_ago = self._gc_days_ago(df)
        if gc_ago is not None and gc_ago <= 5:
            score += CB_SCORE_GC_BONUS; reasons.append(f"GC {gc_ago}일전")

        # 거감음봉 보너스
        if self._is_gge(df):
            score += CB_SCORE_GGE_BONUS; reasons.append("거감음봉")

        # OBV 보너스
        if self._check_obv(df):
            score += CB_SCORE_OBV_BONUS; reasons.append("OBV bull")

        # 과열 감점
        if rsi > CB_OVERHEAT_RSI:
            score -= 10; reasons.append(f"과열 RSI>{CB_OVERHEAT_RSI}")

        score = max(0, min(score, 110))
        info = self.stock_map.get(code)
        name = info.name if info else stock.get("name", code)

        return {
            "code": code, "name": name,
            "score": round(score, 1), "rsi": round(rsi, 1),
            "alignment": "정배열" if ma8 > ma33 else "혼합",
            "pool_type": stock.get("pool_type", "unknown"),
            "reasons": reasons, "note": "",
        }

    # ─── 헬퍼 ───

    def _calc_rsi(self, df, period=14):
        import numpy as np
        delta = df["close"].diff()
        gain = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period).mean()
        loss = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period).mean()
        rs = gain / loss.replace(0, float("nan"))
        rsi = 100 - 100 / (1 + rs)
        val = rsi.iloc[-1]
        return round(float(val), 1) if pd.notna(val) else 50.0

    def _gc_days_ago(self, df):
        ma8 = df["close"].rolling(8).mean()
        ma33 = df["close"].rolling(33).mean()
        for i in range(len(df)-1, max(len(df)-21, 0), -1):
            if i < 1: break
            if ma8.iloc[i] > ma33.iloc[i] and ma8.iloc[i-1] <= ma33.iloc[i-1]:
                return len(df) - 1 - i
        return None

    def _is_gge(self, df):
        if len(df) < 6: return False
        last = df.iloc[-1]
        prev_mean = df["volume"].tail(6).iloc[:-1].mean()
        return (last["volume"] < prev_mean * 0.25 and last["close"] < last["open"])

    def _check_obv(self, df):
        import numpy as np
        if len(df) < 20: return False
        r = df.tail(20)
        ps = np.polyfit(range(20), r["close"].values, 1)[0]
        obv = (np.sign(r["close"].diff()) * r["volume"]).cumsum()
        os_ = np.polyfit(range(len(obv)), obv.values, 1)[0]
        return ps < 0 and os_ > 0

    def _load_ohlcv(self, code):
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
