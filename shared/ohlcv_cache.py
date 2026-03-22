"""
PADO v2 — OHLCV 공유 캐시
===========================
싱글톤 패턴. chart_engine / volume_engine / market_engine이 동일 데이터 사용.
한 번 로드하면 파이프라인 종료까지 메모리 유지 (~40MB).

사용법:
    cache = OHLCVCache.instance()
    cache.preload_all()
    df = cache.get(code)           # tail 120일
    tv = cache.tv_sidecar          # 거래대금 변화율용 사전
"""

import pandas as pd
import numpy as np
from pathlib import Path

from config import OHLCV_DIR, setup_logging
from shared.stock_map import load_stock_map

logger = setup_logging().getChild("ohlcv_cache")

# 캐시 보존 일수 (120일이면 ma45 + 75일 여유)
_CACHE_DAYS = 120


class OHLCVCache:
    """OHLCV 싱글톤 캐시."""

    _instance: "OHLCVCache | None" = None

    def __init__(self):
        self._store: dict[str, pd.DataFrame] = {}
        self._loaded = False
        self.tv_sidecar: dict[str, dict] = {}  # {code: {sector, tv_today, tv_ma20, change_pct}}

    @classmethod
    def instance(cls) -> "OHLCVCache":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        """테스트 또는 다음 날 초기화."""
        cls._instance = None

    @property
    def loaded(self) -> bool:
        return self._loaded

    def preload_all(self, ohlcv_dir: Path | None = None):
        """전종목 OHLCV 로드 + 거래대금 사이드카 수집."""
        if self._loaded:
            return

        d = ohlcv_dir or OHLCV_DIR
        stock_map = load_stock_map()
        codes = list(stock_map.keys())
        loaded = 0
        errors = 0

        logger.info(f"OHLCV 캐시 로딩 시작: {len(codes)}종목")

        for code in codes:
            p = d / f"{code}.csv"
            if not p.exists():
                continue
            try:
                df = self._read_csv(p)
                if df is not None and len(df) >= 5:
                    # tail만 보관 (메모리 절약)
                    self._store[code] = df.tail(_CACHE_DAYS).reset_index(drop=True)

                    # 거래대금 사이드카 수집
                    self._collect_tv(code, df, stock_map.get(code))
                    loaded += 1
            except Exception:
                errors += 1

        self._loaded = True
        mem_mb = sum(df.memory_usage(deep=True).sum() for df in self._store.values()) / 1024 / 1024
        logger.info(f"OHLCV 캐시 완료: {loaded}종목 로드, {errors} 에러, ~{mem_mb:.0f}MB")

    def get(self, code: str) -> pd.DataFrame | None:
        """캐시에서 OHLCV 반환. 없으면 None."""
        return self._store.get(code)

    def get_all_codes(self) -> list[str]:
        return list(self._store.keys())

    def invalidate(self):
        """메모리 해제."""
        self._store.clear()
        self.tv_sidecar.clear()
        self._loaded = False
        logger.info("OHLCV 캐시 무효화")

    # ── 내부 ──

    def _read_csv(self, path: Path) -> pd.DataFrame | None:
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
            df.columns = [c.strip().lower() for c in df.columns]
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)
            for col in ("open", "high", "low", "close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["volume"] = pd.to_numeric(
                df.get("volume", 0), errors="coerce"
            ).fillna(0).astype(int)
            return df.dropna(subset=["close"])
        except Exception:
            return None

    def _collect_tv(self, code: str, df: pd.DataFrame, stock=None):
        """거래대금 변화율 사이드카 데이터 수집."""
        if len(df) < 25:
            return
        try:
            recent = df.tail(25)
            tv = recent["close"] * recent["volume"]  # 거래대금
            tv_today = float(tv.iloc[-1])
            tv_ma20 = float(tv.tail(21).iloc[:-1].mean())  # 오늘 제외 20일 평균

            if tv_ma20 > 0:
                change_pct = (tv_today - tv_ma20) / tv_ma20 * 100
            else:
                change_pct = 0.0

            sector = stock.sector if stock else ""
            self.tv_sidecar[code] = {
                "sector": sector,
                "tv_today": tv_today,
                "tv_ma20": tv_ma20,
                "change_pct": round(change_pct, 1),
            }
        except Exception:
            pass
