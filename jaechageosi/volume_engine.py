"""
거래량 채널 — 자금 검증
========================
OHLCV 패턴(65점) + 수급 API(35점). flow_state 분류 포함.
"""

import numpy as np
import pandas as pd
from pathlib import Path

from config import OHLCV_DIR, API_SLEEP_KIWOOM, setup_logging
from jaechageosi.result_types import VolumeResult, FlowState

logger = setup_logging().getChild("volume_engine")


class VolumeEngine:

    def __init__(self, ohlcv_dir: Path | None = None, api=None):
        self.ohlcv_dir = ohlcv_dir or OHLCV_DIR
        self.api = api  # KiwoomAPI (수급 조회용)

    def score_single(self, code: str, df: pd.DataFrame | None = None) -> VolumeResult | None:
        if df is None:
            df = self._load_ohlcv(code)
        if df is None or len(df) < 30:
            return None

        obv_div = self._calc_obv_divergence(df)
        gge = self._is_gge_strict(df)
        dryup = self._calc_dryup_days(df)
        explosion = self._calc_explosion_ratio(df)
        trap = self._detect_trap(df)
        bearish_exp = self._is_bearish_explosion(df)

        # OHLCV 점수 (65점)
        ohlcv_score = 0
        reasons = []

        # ── 기본 거래량 건강도 (15점) ──
        base_score, base_reasons = self._calc_volume_health(df)
        ohlcv_score += base_score
        reasons.extend(base_reasons)

        # ── 특수 패턴 (50점) ──
        if obv_div:
            ohlcv_score += 15; reasons.append("OBV bull div +15")
        if gge:
            ohlcv_score += 12; reasons.append("거감음봉(5일선) +12")
        if bearish_exp:
            ohlcv_score += 12; reasons.append("음봉폭발 +12")
        if dryup >= 5:
            ohlcv_score += 8; reasons.append(f"급감 {dryup}일 +8")
        elif dryup >= 3:
            ohlcv_score += 4; reasons.append(f"급감 {dryup}일 +4")

        # 추격 감점
        if self._is_chase_pattern(df):
            ohlcv_score -= 10; reasons.append("양봉폭발 추격 -10")
        if trap:
            ohlcv_score -= 5; reasons.append("trap_flag -5")

        ohlcv_score = max(0, min(ohlcv_score, 65))

        # 수급 API (35점)
        api_score = 0
        inst_foreign = False
        short_ratio = 0.0

        if self.api:
            try:
                supply = self._check_supply(code)
                api_score = supply.get("score", 0)
                inst_foreign = supply.get("inst_foreign_5d", False)
                short_ratio = supply.get("short_ratio", 0.0)
                reasons.extend(supply.get("reasons", []))
            except Exception as e:
                logger.warning(f"수급 조회 실패 {code}: {e}")

        total = max(0, min(ohlcv_score + api_score, 100))
        flow = self._classify_flow(obv_div, dryup, explosion, bearish_exp, trap)

        return VolumeResult(
            code=code, score=total, flow_state=flow,
            obv_bull_div=obv_div, gge_strict=gge, dryup_days=dryup,
            explosion_ratio=explosion, inst_foreign_5d=inst_foreign,
            short_ratio=short_ratio, trap_flag=trap, reasons=reasons,
        )

    # ─────────────────────────────────────
    # OHLCV 패턴
    # ─────────────────────────────────────

    # ─────────────────────────────────────
    # 기본 거래량 건강도 (15점)
    # ─────────────────────────────────────

    def _calc_volume_health(self, df: pd.DataFrame) -> tuple[int, list[str]]:
        """일반적 거래량 건강 상태 평가. 극단 패턴 아닌 기본 점수."""
        if len(df) < 21:
            return 0, []

        score = 0
        reasons = []
        last = df.iloc[-1]
        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]

        if vol_ma20 <= 0:
            return 0, []

        vol_ratio = last["volume"] / vol_ma20

        # 1) 거래량 추세 (최근 5일 평균 vs MA20)
        vol_5d = df["volume"].tail(5).mean()
        trend_ratio = vol_5d / vol_ma20

        if 0.5 <= trend_ratio <= 2.0:
            score += 5; reasons.append(f"거래량 안정 +5")
        elif trend_ratio > 2.0:
            score += 3; reasons.append(f"거래량 증가 +3")
        # 극단적 급감은 별도 패턴에서 처리

        # 2) 양봉일 거래량이 음봉일보다 많은지 (5일)
        recent = df.tail(10)
        up_days = recent[recent["close"] >= recent["open"]]
        down_days = recent[recent["close"] < recent["open"]]
        if len(up_days) > 0 and len(down_days) > 0:
            up_vol = up_days["volume"].mean()
            dn_vol = down_days["volume"].mean()
            if up_vol > dn_vol * 1.3:
                score += 5; reasons.append("양봉>음봉 거래량 +5")

        # 3) 거래량이 완전히 죽지 않았는지 (최소 기준)
        if vol_ratio >= 0.3:
            score += 5; reasons.append("거래량 존재 +5")

        return min(score, 15), reasons

    # ─────────────────────────────────────
    # OHLCV 패턴
    # ─────────────────────────────────────

    def _calc_obv_divergence(self, df: pd.DataFrame) -> bool:
        """가격↓ OBV↑ = 바닥 탈출 전조."""
        if len(df) < 20:
            return False
        recent = df.tail(20)
        price_slope = np.polyfit(range(20), recent["close"].values, 1)[0]
        obv = (np.sign(recent["close"].diff()) * recent["volume"]).cumsum()
        obv_slope = np.polyfit(range(len(obv)), obv.values, 1)[0]
        return price_slope < 0 and obv_slope > 0

    def _is_gge_strict(self, df: pd.DataFrame) -> bool:
        """거감음봉 + 5일선 이격 ≤5% (유목민 핵심)."""
        if len(df) < 6:
            return False
        last = df.iloc[-1]
        prev_vol_mean = df["volume"].tail(6).iloc[:-1].mean()
        vol_collapse = last["volume"] < prev_vol_mean * 0.25
        is_bearish = last["close"] < last["open"]
        ma5 = df["close"].rolling(5).mean().iloc[-1]
        gap = abs(last["close"] - ma5) / ma5 if ma5 > 0 else 1
        return vol_collapse and is_bearish and gap <= 0.05

    def _calc_dryup_days(self, df: pd.DataFrame) -> int:
        """거래량 MA20의 25% 이하 연속일."""
        if len(df) < 21:
            return 0
        ma20 = df["volume"].rolling(20).mean().iloc[-1]
        threshold = ma20 * 0.25
        count = 0
        for i in range(len(df) - 1, max(len(df) - 21, 0), -1):
            if df.iloc[i]["volume"] < threshold:
                count += 1
            else:
                break
        return count

    def _calc_explosion_ratio(self, df: pd.DataFrame) -> float:
        """직전 거래량 폭발 배율 (MA20 대비)."""
        if len(df) < 21:
            return 0.0
        ma20 = df["volume"].rolling(20).mean().iloc[-1]
        if ma20 <= 0:
            return 0.0
        last_vol = df.iloc[-1]["volume"]
        return round(last_vol / ma20, 1)

    def _is_bearish_explosion(self, df: pd.DataFrame) -> bool:
        """음봉 + 거래량 3배 이상."""
        if len(df) < 21:
            return False
        last = df.iloc[-1]
        ma20 = df["volume"].rolling(20).mean().iloc[-1]
        return last["close"] < last["open"] and last["volume"] > ma20 * 3

    def _is_chase_pattern(self, df: pd.DataFrame) -> bool:
        """양봉 폭발 추격: 양봉 + 거래량 3배+."""
        if len(df) < 21:
            return False
        last = df.iloc[-1]
        ma20 = df["volume"].rolling(20).mean().iloc[-1]
        return last["close"] > last["open"] and last["volume"] > ma20 * 3

    def _detect_trap(self, df: pd.DataFrame) -> bool:
        """양봉폭발 + 윗꼬리 큼 + 체결강도 둔화."""
        if len(df) < 2:
            return False
        last = df.iloc[-1]
        body = abs(last["close"] - last["open"])
        upper_wick = last["high"] - max(last["close"], last["open"])
        if body > 0 and upper_wick / body > 1.5 and self._is_chase_pattern(df):
            return True
        return False

    # ─────────────────────────────────────
    # flow_state 분류
    # ─────────────────────────────────────

    def _classify_flow(self, obv_div, dryup, explosion, bearish_exp, trap) -> FlowState:
        if trap or (explosion > 3 and not obv_div and not bearish_exp):
            return "chasing"
        if obv_div and dryup >= 2 and explosion >= 2:
            return "reignite"
        if dryup >= 3 and explosion < 2:
            return "digest"
        if explosion >= 3:
            return "ignite"
        return "accumulation"

    # ─────────────────────────────────────
    # 수급 API
    # ─────────────────────────────────────

    def _check_supply(self, code: str) -> dict:
        """ka10059 기관/외인 + ka10014 공매도 → 35점."""
        import time
        score = 0
        reasons = []
        inst_foreign = False
        short_ratio = 0.0

        # ── 1) 기관/외인 수급 (ka10059) ──
        try:
            time.sleep(API_SLEEP_KIWOOM)
            trends = self.api.get_investor_trend(code, days=5) if self.api else []
            if trends:
                foreign_sum = sum(t.get("foreign", 0) for t in trends)
                inst_sum = sum(t.get("institution", 0) for t in trends)

                if foreign_sum > 0 and inst_sum > 0:
                    score += 15; inst_foreign = True
                    reasons.append(f"외인+기관 순매수 +15")
                elif foreign_sum > 0:
                    score += 8
                    reasons.append(f"외인 순매수 +8")
                elif inst_sum > 0:
                    score += 6
                    reasons.append(f"기관 순매수 +6")
                elif foreign_sum < 0 and inst_sum < 0:
                    score -= 10
                    reasons.append(f"외인+기관 순매도 -10")
        except Exception as e:
            logger.debug(f"수급 API 실패 {code}: {e}")

        # ── 2) 공매도 (ka10014) ──
        try:
            time.sleep(API_SLEEP_KIWOOM)
            shorts = self.api.get_short_selling(code, days=5) if self.api else []
            if shorts:
                avg_ratio = sum(s.get("short_ratio", 0) for s in shorts) / len(shorts)
                short_ratio = round(avg_ratio, 2)
                if short_ratio < 3:
                    score += 7; reasons.append(f"공매도 {short_ratio}% (낮음) +7")
                elif short_ratio > 5:
                    score -= 5; reasons.append(f"공매도 {short_ratio}% (높음) -5")
        except Exception as e:
            logger.debug(f"공매도 조회 실패 {code}: {e}")

        return {"score": max(0, min(score, 35)), "inst_foreign_5d": inst_foreign,
                "short_ratio": short_ratio, "reasons": reasons[:3]}

    def _load_ohlcv(self, code: str) -> pd.DataFrame | None:
        # v2: 캐시 우선, 없으면 파일 직접 로드
        from shared.ohlcv_cache import OHLCVCache
        cache = OHLCVCache.instance()
        if cache.loaded:
            return cache.get(code)

        p = self.ohlcv_dir / f"{code}.csv"
        if not p.exists():
            return None
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            df.columns = [c.strip().lower() for c in df.columns]
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
            return df.dropna(subset=["close"])
        except Exception:
            return None
