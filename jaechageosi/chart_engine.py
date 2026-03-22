"""
차트 채널 — 자리 발굴
======================
전종목 OHLCV 스캔 → 신호 발견 + chart_state 분류 + 100점 평가.
API 호출 0. 로컬 CSV만 사용. ~2분 소요.
"""

import numpy as np
import pandas as pd
from pathlib import Path

from config import (
    OHLCV_DIR, SCAN_MIN_SIGNALS, SCAN_MIN_SCORE, SCAN_MAX_CANDIDATES,
    setup_logging,
)
from shared.stock_map import load_stock_map
from jaechageosi.result_types import ChartResult, ChartState

logger = setup_logging().getChild("chart_engine")


class ChartEngine:

    def __init__(self, ohlcv_dir: Path | None = None):
        self.ohlcv_dir = ohlcv_dir or OHLCV_DIR
        self.stock_map = load_stock_map()

    # ─────────────────────────────────────
    # 공개 API
    # ─────────────────────────────────────

    def scan_all(self) -> list[ChartResult]:
        """전종목 스캔 → 신호 있는 종목만 (점수순, 최대 SCAN_MAX_CANDIDATES건)."""
        results = []
        codes = list(self.stock_map.keys())
        logger.info(f"차트 스캔 시작: {len(codes)}종목")

        for code in codes:
            try:
                r = self.score_single(code)
                if r and len(r.signal_family) >= SCAN_MIN_SIGNALS and r.score >= SCAN_MIN_SCORE:
                    results.append(r)
            except Exception as e:
                logger.debug(f"차트 스캔 실패 {code}: {e}")

        results.sort(key=lambda x: x.score, reverse=True)
        results = results[:SCAN_MAX_CANDIDATES]
        logger.info(f"차트 스캔 완료: {len(results)}건 후보")
        return results

    def score_single(self, code: str) -> ChartResult | None:
        """단일 종목 차트 100점 평가."""
        df = self._load_ohlcv(code)
        if df is None or len(df) < 60:
            return None

        signals = self._detect_signals(df)
        state = self._classify_state(df)
        alignment = self._calc_ma_alignment(df)
        rsi = self._calc_rsi(df)
        support, resistance = self._calc_support_resistance(df)
        gc_ago = self._gc_days_ago(df)
        score, reasons = self._score_breakdown(df, signals, alignment, rsi, gc_ago)

        # 진입가/손절가/목표가 (v1.1: chart에서 계산)
        last = df.iloc[-1]
        ma8 = df["close"].rolling(8).mean().iloc[-1]
        ma33 = df["close"].rolling(33).mean().iloc[-1]
        entry = round(max(ma8, ma33) * 1.01, 0) if ma33 > 0 else None
        stop = round(ma33 * 0.97, 0) if ma33 > 0 else None
        target = round(resistance * 0.98, 0) if resistance > 0 else None

        return ChartResult(
            code=code, score=score, signal_family=signals,
            chart_state=state, ma_alignment=alignment, rsi=rsi,
            nearest_support=support, nearest_resistance=resistance,
            gc_days_ago=gc_ago,
            entry_price=entry, stop_loss=stop, target_price=target,
            reasons=reasons,
        )

    # ─────────────────────────────────────
    # 신호 탐지
    # ─────────────────────────────────────

    def _detect_signals(self, df: pd.DataFrame) -> list[str]:
        signals = []
        last = df.iloc[-1]
        ma8 = df["close"].rolling(8).mean()
        ma33 = df["close"].rolling(33).mean()

        # GC(8→33): 최근 5일 내
        for i in range(-5, 0):
            if len(df) + i < 1:
                continue
            if ma8.iloc[i] > ma33.iloc[i] and ma8.iloc[i - 1] <= ma33.iloc[i - 1]:
                signals.append("gc")
                break

        # 역배열→정배열 전환
        if len(df) >= 10:
            prev_align = self._alignment_at(df, -5)
            curr_align = self._alignment_at(df, -1)
            if prev_align in ("역배열", "혼합") and curr_align == "정배열":
                signals.append("reversal_to_bull")

        # RSI 30↓ 반등
        rsi_series = self._calc_rsi_series(df)
        if len(rsi_series) >= 3:
            if rsi_series.iloc[-3] < 30 and rsi_series.iloc[-1] > 30:
                signals.append("rsi_reclaim")

        # 1차 파동 (바닥 탈출)
        if self._check_wave1(df):
            signals.append("wave1")

        # 2차 파동 (재폭발)
        if self._check_wave2(df):
            signals.append("wave2")

        return signals

    def _check_wave1(self, df: pd.DataFrame) -> bool:
        """1차 파동: 2년 최저 5% 이내 + 60일 내 30%+ 하락 + 거래량 급감→폭발."""
        if len(df) < 120:
            return False
        last = df.iloc[-1]
        low_2y = df["low"].tail(min(len(df), 500)).min()
        if last["close"] > low_2y * 1.05:
            return False
        high_60d = df["high"].tail(60).max()
        if (high_60d - last["close"]) / high_60d < 0.30:
            return False
        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]
        recent_vols = df["volume"].tail(5)
        dryup = (recent_vols < vol_ma20 * 0.25).sum()
        if dryup < 2:
            return False
        if last["volume"] < vol_ma20 * 3:
            return False
        return True

    def _check_wave2(self, df: pd.DataFrame) -> bool:
        """2차 파동: 5배 폭발 → 20일 내 급감 → RSI<50 → 재폭발."""
        if len(df) < 30:
            return False
        vol_ma20 = df["volume"].rolling(20).mean()
        recent = df.tail(20)
        explosions = recent[recent["volume"] > vol_ma20.tail(20) * 5]
        if len(explosions) < 1:
            return False
        rsi = self._calc_rsi(df)
        return rsi < 50

    # ─────────────────────────────────────
    # 상태 분류
    # ─────────────────────────────────────

    def _classify_state(self, df: pd.DataFrame) -> ChartState:
        last = df.iloc[-1]
        rsi = self._calc_rsi(df)
        ma8 = df["close"].rolling(8).mean().iloc[-1]
        ma33 = df["close"].rolling(33).mean().iloc[-1]
        ma5_gap = (last["close"] - df["close"].rolling(5).mean().iloc[-1]) / last["close"]
        high_20d = df["high"].tail(20).max()
        pullback = (high_20d - last["close"]) / high_20d if high_20d > 0 else 0

        if rsi > 70 and ma5_gap > 0.10:
            return "extended"
        if last["close"] > ma8 > ma33 and pullback < 0.03:
            return "breakout"
        if 0.03 <= pullback <= 0.12 and last["close"] >= ma33 * 0.98:
            return "pullback"
        return "bottom"

    # ─────────────────────────────────────
    # 이평 배열
    # ─────────────────────────────────────

    def _calc_ma_alignment(self, df: pd.DataFrame) -> str:
        return self._alignment_at(df, -1)

    def _alignment_at(self, df: pd.DataFrame, idx: int) -> str:
        if len(df) < 45:
            return "혼합"
        ma8 = df["close"].rolling(8).mean().iloc[idx]
        ma33 = df["close"].rolling(33).mean().iloc[idx]
        ma45 = df["close"].rolling(45).mean().iloc[idx]
        if ma8 > ma33 > ma45:
            return "정배열"
        if ma8 < ma33 < ma45:
            return "역배열"
        return "혼합"

    # ─────────────────────────────────────
    # 지지/저항
    # ─────────────────────────────────────

    def _calc_support_resistance(self, df: pd.DataFrame) -> tuple[float, float]:
        """최근 120일 로컬 최저/최고에서 지지선·저항선 추출."""
        last_close = df.iloc[-1]["close"]
        recent = df.tail(min(len(df), 120))

        # 간이 방식: 최근 저점/고점 클러스터
        lows = recent["low"].values
        highs = recent["high"].values

        support = 0.0
        resistance = last_close * 1.20  # 기본값

        # 현재가 아래 지지선
        below = lows[lows < last_close]
        if len(below) > 0:
            support = float(np.percentile(below, 75))  # 75% 수준

        # 현재가 위 저항선
        above = highs[highs > last_close]
        if len(above) > 0:
            resistance = float(np.percentile(above, 25))  # 25% 수준

        return round(support, 0), round(resistance, 0)

    # ─────────────────────────────────────
    # GC 경과일
    # ─────────────────────────────────────

    def _gc_days_ago(self, df: pd.DataFrame) -> int | None:
        ma8 = df["close"].rolling(8).mean()
        ma33 = df["close"].rolling(33).mean()
        for i in range(len(df) - 1, max(len(df) - 21, 0), -1):
            if i < 1:
                break
            if ma8.iloc[i] > ma33.iloc[i] and ma8.iloc[i - 1] <= ma33.iloc[i - 1]:
                return len(df) - 1 - i
        return None

    # ─────────────────────────────────────
    # RSI
    # ─────────────────────────────────────

    def _calc_rsi(self, df: pd.DataFrame, period: int = 14) -> float:
        s = self._calc_rsi_series(df, period)
        return round(float(s.iloc[-1]), 1) if len(s) > 0 and not np.isnan(s.iloc[-1]) else 50.0

    def _calc_rsi_series(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        delta = df["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    # ─────────────────────────────────────
    # 100점 스코어링
    # ─────────────────────────────────────

    def _score_breakdown(self, df, signals, alignment, rsi, gc_ago) -> tuple[int, list[str]]:
        score = 0
        reasons = []

        # 이평 배열 (25점)
        if alignment == "정배열":
            score += 25; reasons.append("정배열 +25")
        elif alignment == "혼합":
            score += 10; reasons.append("혼합 +10")
            if "reversal_to_bull" in signals:
                score += 10; reasons.append("혼합→정배열 전환 +10")

        # GC(8→33) (20점, decay)
        if gc_ago is not None:
            gc_pts = max(0, 20 - gc_ago * 3)  # 0일:20, 1일:17, 3일:11, 5일:5
            score += gc_pts; reasons.append(f"GC {gc_ago}일전 +{gc_pts}")

        # 이평선 지지 (20점)
        last = df.iloc[-1]["close"]
        ma33 = df["close"].rolling(33).mean().iloc[-1]
        ma8 = df["close"].rolling(8).mean().iloc[-1]
        if ma33 > 0 and abs(last - ma33) / ma33 < 0.02:
            score += 20; reasons.append("33일선 지지 +20")
        elif ma8 > 0 and abs(last - ma8) / ma8 < 0.02:
            score += 15; reasons.append("8일선 지지 +15")

        # RSI (20점)
        if 30 <= rsi <= 50:
            score += 20; reasons.append(f"RSI {rsi} (30~50) +20")
        elif 50 < rsi <= 60:
            score += 15; reasons.append(f"RSI {rsi} (50~60) +15")
        elif 60 < rsi <= 70:
            score += 5; reasons.append(f"RSI {rsi} (60~70) +5")
        elif rsi > 70:
            score -= 10; reasons.append(f"RSI {rsi} (>70 과열) -10")

        # 위 매물대 (15점)
        _, resistance = self._calc_support_resistance(df)
        overhead_pct = (resistance - last) / last if last > 0 else 0
        if overhead_pct > 0.15:
            score += 15; reasons.append("위 매물대 여유 +15")
        elif overhead_pct > 0.08:
            score += 10; reasons.append("위 매물대 보통 +10")

        # extended 감점 (-15)
        state = self._classify_state(df)
        if state == "extended":
            score -= 15; reasons.append("extended 과열 -15")

        return max(0, min(score, 100)), reasons

    # ─────────────────────────────────────
    # OHLCV 로드
    # ─────────────────────────────────────

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
