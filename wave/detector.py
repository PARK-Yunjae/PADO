"""
파동 감지 — 전종목 스캔
========================
1차 파동 (바닥 탈출) + 2차 파동 (재폭발) 탐지.
일봉 기반. 장 마감 후 실행, 다음날 08:30에 알림.
"""

import numpy as np
import pandas as pd
from pathlib import Path

from config import OHLCV_DIR, setup_logging
from shared.schemas import WaveSignal
from shared.stock_map import load_stock_map, get_stock
from shared import storage

logger = setup_logging().getChild("wave_detector")


class WaveDetector:

    def __init__(self, ohlcv_dir: Path | None = None):
        self.ohlcv_dir = ohlcv_dir or OHLCV_DIR
        self.stock_map = load_stock_map()

    def scan_all(self, date: str) -> list[WaveSignal]:
        """전종목 파동 스캔 → 시그널 리스트 + DB 저장."""
        signals = []
        codes = list(self.stock_map.keys())
        logger.info(f"파동 스캔 시작: {len(codes)}종목")

        for code in codes:
            try:
                df = self._load_ohlcv(code)
                if df is None or len(df) < 120:
                    continue

                w1 = self._detect_wave1(code, df, date)
                if w1:
                    signals.append(w1)

                w2 = self._detect_wave2(code, df, date)
                if w2:
                    signals.append(w2)

                w3 = self._detect_wave3(code, df, date)
                if w3:
                    signals.append(w3)

            except Exception as e:
                logger.debug(f"파동 스캔 실패 {code}: {e}")

        # DB 저장
        for s in signals:
            storage.save_wave_signal({
                "code": s.code, "name": s.name, "wave_type": s.wave_type,
                "detect_date": s.detect_date, "strength": s.strength,
                "wave_count": s.wave_count,
            })

        logger.info(f"파동 스캔 완료: {len(signals)}건 감지")
        return signals

    # ─────────────────────────────────────
    # 1차 파동: 바닥 탈출
    # ─────────────────────────────────────

    def _detect_wave1(self, code: str, df: pd.DataFrame, date: str) -> WaveSignal | None:
        """
        조건:
        ① 2년 최저가 5% 이내
        ② 60일 내 30%+ 하락
        ③ 거래량 급감 2일+ (MA20의 25% 이하)
        ④ 거래량 3배+ 폭발
        RSI 15~55 필터.
        """
        last = df.iloc[-1]
        low_2y = df["low"].tail(min(len(df), 500)).min()

        # ① 2년 최저 5% 이내
        if last["close"] > low_2y * 1.05:
            return None

        # ② 60일 내 30%+ 하락
        high_60d = df["high"].tail(60).max()
        if high_60d <= 0 or (high_60d - last["close"]) / high_60d < 0.30:
            return None

        # ③ 거래량 급감 2일+
        vol_ma20 = df["volume"].rolling(20).mean()
        recent_5 = df.tail(5)
        dryup_count = (recent_5["volume"] < vol_ma20.tail(5) * 0.25).sum()
        if dryup_count < 2:
            return None

        # ④ 거래량 3배+ 폭발 (마지막 날)
        if vol_ma20.iloc[-1] <= 0:
            return None
        explosion = last["volume"] / vol_ma20.iloc[-1]
        if explosion < 3.0:
            return None

        # RSI 필터
        rsi = self._calc_rsi(df)
        if rsi < 15 or rsi > 55:
            return None

        # OBV 다이버전스 체크
        obv_bull = self._check_obv_divergence(df)
        # 거감음봉 체크
        gge = self._check_gge(df)

        # 강도 계산 (0~1)
        strength = min(1.0, (explosion / 10) * 0.4 + (dryup_count / 5) * 0.3 +
                       (0.2 if obv_bull else 0) + (0.1 if gge else 0))

        # 파동 차수 (DB에서 이전 횟수 조회)
        prev_count = storage.get_wave_count(code, "wave1")
        wave_count = prev_count + 1

        stock = get_stock(code)
        name = stock.name if stock else code

        reasons = [f"2년 최저 {(last['close']/low_2y-1)*100:.1f}%",
                   f"60일 고점 대비 -{(1-last['close']/high_60d)*100:.0f}%",
                   f"급감 {dryup_count}일", f"폭발 {explosion:.1f}배"]
        if obv_bull:
            reasons.append("OBV bull div")
        if gge:
            reasons.append("거감음봉")

        return WaveSignal(
            code=code, name=name, wave_type="wave1",
            detect_date=date, strength=round(strength, 2),
            wave_count=wave_count, obv_bull=obv_bull, gge=gge,
            reasons=reasons,
        )

    # ─────────────────────────────────────
    # 2차 파동: 재폭발
    # ─────────────────────────────────────

    def _detect_wave2(self, code: str, df: pd.DataFrame, date: str) -> WaveSignal | None:
        """
        조건:
        ① 거래량 5배+ 폭발 (과거 20일 내)
        ② 이후 10일 내 20% 이하 급감
        ③ RSI < 50
        ④ 이후 10일 내 3배+ 재폭발
        """
        if len(df) < 30:
            return None

        vol_ma20 = df["volume"].rolling(20).mean()
        recent_20 = df.tail(20)

        # ① 5배 폭발 찾기
        first_explosion_idx = None
        for i in range(len(recent_20)):
            idx = recent_20.index[i]
            if vol_ma20.loc[idx] > 0 and recent_20.loc[idx, "volume"] > vol_ma20.loc[idx] * 5:
                first_explosion_idx = i
                break

        if first_explosion_idx is None:
            return None

        # ② 폭발 이후 급감
        after_explosion = recent_20.iloc[first_explosion_idx:]
        if len(after_explosion) < 3:
            return None

        peak_vol = after_explosion["volume"].iloc[0]
        has_dryup = any(v < peak_vol * 0.20 for v in after_explosion["volume"].values[1:])
        if not has_dryup:
            return None

        # ③ RSI < 50
        rsi = self._calc_rsi(df)
        if rsi >= 50:
            return None

        # ④ 재폭발 (마지막 날)
        last = df.iloc[-1]
        if vol_ma20.iloc[-1] <= 0:
            return None
        re_explosion = last["volume"] / vol_ma20.iloc[-1]
        if re_explosion < 3.0:
            return None

        gge = self._check_gge(df)
        obv_bull = self._check_obv_divergence(df)

        strength = min(1.0, (re_explosion / 8) * 0.4 + (0.3 if gge else 0.1) +
                       (0.2 if obv_bull else 0) + (0.1 if rsi < 30 else 0))

        prev_count = storage.get_wave_count(code, "wave2")
        wave_count = prev_count + 1

        stock = get_stock(code)
        name = stock.name if stock else code

        reasons = [f"5배 폭발 후 급감", f"재폭발 {re_explosion:.1f}배",
                   f"RSI {rsi:.0f}"]
        if gge:
            reasons.append("거감음봉")
        if rsi < 30:
            reasons.append("RSI<30")

        return WaveSignal(
            code=code, name=name, wave_type="wave2",
            detect_date=date, strength=round(strength, 2),
            wave_count=wave_count, obv_bull=obv_bull, gge=gge,
            reasons=reasons,
        )

    # ─────────────────────────────────────
    # 3차 파동: 약화 (이전 파동 경험 종목의 반복)
    # ─────────────────────────────────────

    def _detect_wave3(self, code: str, df: pd.DataFrame, date: str) -> WaveSignal | None:
        """
        3차 파동 (약화):
        ① 이전에 wave1 또는 wave2가 2회 이상 감지된 종목
        ② 거래량 2배+ 폭발 (1차/2차보다 약한 기준)
        ③ RSI 30~60
        ④ 강도가 이전보다 약함 → 주의 신호
        
        성호전자 사례: 1차→2차→3차로 점점 약해지는 패턴
        """
        # ① 이전 파동 2회 이상
        prev_w1 = storage.get_wave_count(code, "wave1")
        prev_w2 = storage.get_wave_count(code, "wave2")
        total_prev = prev_w1 + prev_w2
        if total_prev < 2:
            return None

        # 이미 이번 날짜에 w1/w2가 감지되었으면 스킵
        # (w3는 w1/w2가 안 걸릴 때만)

        last = df.iloc[-1]
        vol_ma20 = df["volume"].rolling(20).mean().iloc[-1]
        if vol_ma20 <= 0:
            return None

        # ② 2배+ 폭발 (1차=3배, 2차=3배보다 낮은 기준)
        explosion = last["volume"] / vol_ma20
        if explosion < 2.0:
            return None

        # ③ RSI 30~60
        rsi = self._calc_rsi(df)
        if rsi < 30 or rsi > 60:
            return None

        gge = self._check_gge(df)
        obv_bull = self._check_obv_divergence(df)

        # 강도 (약화 반영: 기본 0.2~0.5 범위)
        strength = min(0.5, (explosion / 10) * 0.3 + (0.1 if obv_bull else 0) + (0.1 if gge else 0))

        wave_count = total_prev + 1  # 3차, 4차, ...
        stock = get_stock(code)
        name = stock.name if stock else code

        reasons = [
            f"이전 파동 {total_prev}회 감지",
            f"폭발 {explosion:.1f}배 (약화)",
            f"RSI {rsi:.0f}",
        ]
        if gge:
            reasons.append("거감음봉")

        return WaveSignal(
            code=code, name=name, wave_type="wave3",
            detect_date=date, strength=round(strength, 2),
            wave_count=wave_count, obv_bull=obv_bull, gge=gge,
            reasons=reasons,
        )

    # ─────────────────────────────────────
    # 헬퍼
    # ─────────────────────────────────────

    def _check_obv_divergence(self, df: pd.DataFrame) -> bool:
        if len(df) < 20:
            return False
        recent = df.tail(20)
        price_slope = np.polyfit(range(20), recent["close"].values, 1)[0]
        obv = (np.sign(recent["close"].diff()) * recent["volume"]).cumsum()
        obv_slope = np.polyfit(range(len(obv)), obv.values, 1)[0]
        return price_slope < 0 and obv_slope > 0

    def _check_gge(self, df: pd.DataFrame) -> bool:
        """거감음봉 + 5일선 이격 ≤5%."""
        if len(df) < 6:
            return False
        last = df.iloc[-1]
        prev_mean = df["volume"].tail(6).iloc[:-1].mean()
        vol_drop = last["volume"] < prev_mean * 0.25
        bearish = last["close"] < last["open"]
        ma5 = df["close"].rolling(5).mean().iloc[-1]
        gap = abs(last["close"] - ma5) / ma5 if ma5 > 0 else 1
        return vol_drop and bearish and gap <= 0.05

    def _calc_rsi(self, df: pd.DataFrame, period: int = 14) -> float:
        delta = df["close"].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        val = rsi.iloc[-1]
        return round(float(val), 1) if not np.isnan(val) else 50.0

    def _load_ohlcv(self, code: str) -> pd.DataFrame | None:
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
