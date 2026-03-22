"""
시황 채널 — 오늘 해도 되는가
==============================
시장 전체 1회 판단. 종목 무관. MarketMode 4단계 분류.
v2: 거래대금 변화율 + 3소스 병합 + 뉴스 테마 감지.
"""

import json
import pandas as pd
from pathlib import Path
from collections import Counter

from config import (
    GLOBAL_CSV, MARKET_CALENDAR, MARKET_SCORE_THEME, MARKET_SCORE_CALENDAR,
    MARKET_SCORE_NASDAQ, MARKET_SCORE_KOSPI_GAP,
    TRADING_VALUE_MIN, TRADING_VALUE_CHANGE_MIN,
    setup_logging,
)
from jaechageosi.result_types import MarketResult, MarketMode

logger = setup_logging().getChild("market_engine")

SEASONAL_THEMES = {
    1: "CES(라스베가스), 신년 정책",
    2: "설 연휴, MWC",
    3: "배당락, 게임사 신작",
    4: "실적 시즌, 어닝 서프라이즈",
    5: "5월 효과, 주총 시즌",
    6: "E3/게임쇼, 여름 에어컨",
    7: "실적 시즌(2Q), 반기 리밸런싱",
    8: "잭슨홀, 여름 비수기",
    9: "추석 소비재, 아이폰 신작",
    10: "실적 시즌(3Q), 할로윈",
    11: "블랙프라이데이, 연말 배당",
    12: "윈도우 드레싱, 세금 매도",
}


class MarketEngine:

    def __init__(self, api=None, tv_data: dict | None = None):
        self.api = api
        # tv_data: OHLCVCache.tv_sidecar (거래대금 변화율용)
        self._tv_data = tv_data or {}

    def evaluate(self, date: str, tv_data: dict | None = None) -> MarketResult:
        """시장 전체 1회 판단 → MarketResult."""
        from datetime import datetime
        reasons = []

        if tv_data is not None:
            self._tv_data = tv_data

        # 나스닥 (20점)
        nasdaq_chg = self._get_nasdaq_change()
        nasdaq_score = self._score_nasdaq(nasdaq_chg)
        reasons.append(f"나스닥 {nasdaq_chg:+.2f}% → {nasdaq_score}점")

        # 캘린더 (25점)
        cal_score, cal_reasons = self._score_calendar(date)
        reasons.extend(cal_reasons)

        # 주도 테마 (35점) — v2 3소스 병합
        theme_score, themes = self._score_theme(date)
        if themes:
            reasons.append(f"주도테마: {', '.join(themes)} → {theme_score}점")
        else:
            reasons.append(f"주도테마 불명확 → {theme_score}점")

        # 코스피 이격 (20점)
        kospi_gap = self._get_kospi_gap()
        gap_score = self._score_kospi_gap(kospi_gap)
        reasons.append(f"코스피 이격 {kospi_gap:+.1f}% → {gap_score}점")

        total = max(0, min(nasdaq_score + cal_score + theme_score + gap_score, 100))
        dangerous = total < 20 or cal_score == 0
        mode = self._classify_mode(total, themes, nasdaq_chg, dangerous)

        month = datetime.strptime(date, "%Y-%m-%d").month if "-" in date else 1
        seasonal = SEASONAL_THEMES.get(month, "")

        return MarketResult(
            date=date, score=total, mode=mode,
            leading_themes=themes, dangerous=dangerous,
            nasdaq_change=nasdaq_chg, kospi_ma20_gap=kospi_gap,
            seasonal_note=seasonal, reasons=reasons,
        )

    # ─────────────────────────────────────
    # 나스닥 / 캘린더 / 코스피 (기존 유지)
    # ─────────────────────────────────────

    def _get_nasdaq_change(self) -> float:
        try:
            df = pd.read_csv(GLOBAL_CSV, parse_dates=["Date"])
            if "NASDAQ" in df.columns and len(df) >= 2:
                last = df["NASDAQ"].dropna().iloc[-1]
                prev = df["NASDAQ"].dropna().iloc[-2]
                return round((last - prev) / prev * 100, 2)
        except Exception:
            pass
        return 0.0

    def _score_nasdaq(self, chg: float) -> int:
        if chg >= 1.0:
            return MARKET_SCORE_NASDAQ
        if chg >= 0:
            return int(MARKET_SCORE_NASDAQ * 0.6)
        if chg >= -1.0:
            return int(MARKET_SCORE_NASDAQ * 0.3)
        if chg >= -2.0:
            return 0
        return -10

    def _score_calendar(self, date: str) -> tuple[int, list[str]]:
        try:
            if MARKET_CALENDAR.exists():
                with open(MARKET_CALENDAR, "r", encoding="utf-8") as f:
                    cal = json.load(f)
                events = [e for e in cal.get("events", []) if e.get("date") == date]
                if any(e.get("danger") for e in events):
                    return 0, ["⚠️ 위험 이벤트: " + events[0].get("name", "")]
                if events:
                    return int(MARKET_SCORE_CALENDAR * 0.8), [f"이벤트: {events[0].get('name')}"]
        except Exception:
            pass
        return MARKET_SCORE_CALENDAR, ["캘린더 이벤트 없음"]

    def _get_kospi_gap(self) -> float:
        try:
            df = pd.read_csv(GLOBAL_CSV, parse_dates=["Date"])
            if "KOSPI" in df.columns and len(df) >= 20:
                kospi = df["KOSPI"].dropna()
                ma20 = kospi.rolling(20).mean().iloc[-1]
                last = kospi.iloc[-1]
                return round((last - ma20) / ma20 * 100, 1)
        except Exception:
            pass
        return 0.0

    def _score_kospi_gap(self, gap: float) -> int:
        if -3 <= gap <= 0:
            return MARKET_SCORE_KOSPI_GAP
        if 0 < gap <= 3:
            return int(MARKET_SCORE_KOSPI_GAP * 0.75)
        if gap > 5 or gap < -5:
            return int(MARKET_SCORE_KOSPI_GAP * 0.25)
        return int(MARKET_SCORE_KOSPI_GAP * 0.5)

    def _classify_mode(self, total, themes, nasdaq, dangerous) -> MarketMode:
        if dangerous:
            return "risk_off"
        if len(themes) >= 2 and total >= 60:
            return "theme_strong"
        if nasdaq >= 1.0 and total >= 50:
            return "index_rally"
        if total >= 35:
            return "mixed"
        return "risk_off"

    # ─────────────────────────────────────
    # v2: 주도테마 3소스 병합 (35점)
    # ─────────────────────────────────────

    def _score_theme(self, date: str) -> tuple[int, list[str]]:
        """3소스 병합 → 35점 만점 + leading_themes (canon)."""
        from shared.theme_taxonomy import merge_theme_sources

        # 소스1: 키움 테마 API
        kiwoom_themes = self._get_kiwoom_themes()

        # 소스2: 거래대금 변화율 → 주도섹터
        change_sectors = self._get_volume_change_sectors()

        # 소스3: 뉴스 키워드 → 오늘 이슈
        news_themes = self._detect_news_themes(date)

        score, themes = merge_theme_sources(kiwoom_themes, change_sectors, news_themes)

        logger.info(
            f"시황테마: 키움{kiwoom_themes} 변화율{change_sectors} "
            f"뉴스{news_themes} → {themes} ({score}점)"
        )
        return score, themes

    def _get_kiwoom_themes(self) -> list[str]:
        """소스1: 키움 테마 API → 상위 3개 테마명 (원본)."""
        if not self.api:
            return []
        try:
            groups = self.api.get_theme_groups(sort="3", period="1")
            if groups and len(groups) >= 3:
                # 버그 수정: "theme_name" → "name"
                return [g.get("name", g.get("theme_name", "")) for g in groups[:3]]
            return []
        except Exception:
            return []

    def _get_volume_change_sectors(self) -> list[str]:
        """소스2: 거래대금 변화율 TOP → 주도섹터 (canon)."""
        from shared.theme_taxonomy import normalize_sector

        if not self._tv_data:
            return []

        # 필터: 당일 ≥ 30억 AND 변화율 ≥ 100%
        passed = []
        for code, tv in self._tv_data.items():
            if (tv.get("tv_today", 0) >= TRADING_VALUE_MIN
                    and tv.get("change_pct", 0) >= TRADING_VALUE_CHANGE_MIN):
                sector = tv.get("sector", "")
                if sector:
                    passed.append(sector)

        if not passed:
            return []

        # 섹터 → canon 변환 후 빈도 카운트
        canon_counts = Counter()
        for sector in passed:
            canon = normalize_sector(sector)
            if canon:
                canon_counts[canon] += 1

        # 상위 3개
        top3 = [c for c, _ in canon_counts.most_common(3)]
        logger.info(f"거래대금 변화율: {len(passed)}종목 통과, TOP={top3}")
        return top3

    def _detect_news_themes(self, date: str) -> list[str]:
        """소스3: 뉴스 DB에서 오늘 키워드 빈도 → 상위 3개."""
        from shared.theme_taxonomy import NEWS_CANON_MAP

        # 뉴스 키워드 사전
        THEME_KEYWORDS = {
            "반도체": ["반도체", "HBM", "파운드리", "메모리", "DRAM"],
            "2차전지": ["2차전지", "배터리", "양극재", "리튬", "전기차"],
            "바이오": ["바이오", "임상", "신약", "FDA", "제약"],
            "AI": ["AI", "인공지능", "GPU", "엔비디아", "딥러닝"],
            "방산": ["방산", "방위", "무기", "K방산", "수출"],
            "원전": ["원전", "SMR", "소형원자로", "원자력"],
            "로봇": ["로봇", "휴머노이드", "자동화", "협동로봇"],
            "정치": ["대선", "총선", "정책", "규제", "탄핵"],
            "조선": ["조선", "해운", "LNG", "선박", "수주"],
            "금융": ["금리", "은행", "금융", "배당", "증권"],
        }

        try:
            from shared.storage import get_today_news
            titles = [row["title"] for row in get_today_news(date)]
        except Exception:
            return []

        if not titles:
            return []

        counts = {}
        for theme, keywords in THEME_KEYWORDS.items():
            cnt = sum(1 for t in titles for kw in keywords if kw in t)
            if cnt >= 3:
                counts[theme] = cnt

        sorted_themes = sorted(counts.items(), key=lambda x: -x[1])
        return [t for t, _ in sorted_themes[:3]]
