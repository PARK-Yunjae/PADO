"""
재료 채널 — 이유 설명 + 악재 제거
===================================
DART 6단계 + 네이버 뉴스 + Gemini 구조화 프롬프트.
후보 종목에 대해서만 실행 (API 호출 포함).
"""

from config import MAX_MATERIAL_EVAL, setup_logging
from jaechageosi.result_types import MaterialResult, Freshness, CatalystType

logger = setup_logging().getChild("material_engine")


class MaterialEngine:

    def __init__(self, api=None):
        self.api = api
        self._eval_count = 0

    def evaluate(self, code: str, name: str, sector: str = "") -> MaterialResult:
        """후보 종목 재료 100점 평가. 결함 격리 포함."""
        if self._eval_count >= MAX_MATERIAL_EVAL:
            logger.warning(f"일일 최대 평가 초과: {self._eval_count}건")
            return self._neutral(code, "일일 평가 한도 초과")

        self._eval_count += 1

        # 1) DART 공시 (6단계)
        try:
            from checkers.dart_checker import check_dart
            dart = check_dart(code)
        except Exception as e:
            logger.warning(f"DART 실패 {code}: {e}")
            dart = {"grade": 4, "score": 40, "reasons": ["DART 조회 실패"]}

        # 즉시 REJECT 체크
        if dart["grade"] <= 1:
            return MaterialResult(
                code=code, score=0, catalyst_type="unknown",
                freshness="stale", theme_link="", dart_grade=dart["grade"],
                headline_summary="치명적 공시 발견", decay_risk="high",
                reasons=dart.get("reasons", ["DART 극위험"]),
            )

        # 2) 뉴스
        try:
            from checkers.news_checker import check_news
            news = check_news(name)
        except Exception as e:
            logger.warning(f"뉴스 실패 {name}: {e}")
            news = {"items": [], "summary": "뉴스 조회 실패", "score": 0}

        # 3) Gemini AI (v2: 섹터별 프롬프트)
        try:
            from checkers.ai_analyzer import analyze_material
            gemini = analyze_material(code, name, dart, news, sector=sector)
        except Exception as e:
            logger.warning(f"Gemini 실패 {code}: {e}")
            gemini = self._rule_based_fallback(dart, news)

        # v2: theme_link를 canon으로 정규화
        raw_link = gemini.get("theme_link", "")
        try:
            from shared.theme_taxonomy import resolve_keyword
            canon = resolve_keyword(raw_link)
            theme_link = canon if canon else raw_link
        except Exception:
            theme_link = raw_link

        # 점수 합산
        dart_score = dart.get("score", 40)
        news_score = news.get("score", 0)
        freshness_bonus = {"first_seen": 15, "recent": 0, "stale": -10}.get(
            gemini.get("freshness", "recent"), 0
        )
        total = max(0, min(dart_score + news_score + freshness_bonus, 100))

        return MaterialResult(
            code=code, score=total,
            catalyst_type=gemini.get("catalyst_type", "unknown"),
            freshness=gemini.get("freshness", "recent"),
            theme_link=theme_link,
            dart_grade=dart["grade"],
            headline_summary=gemini.get("summary", news.get("summary", "")),
            decay_risk=gemini.get("decay_risk", "mid"),
            reasons=dart.get("reasons", []) + news.get("reasons", []),
        )

    def _neutral(self, code: str, reason: str) -> MaterialResult:
        return MaterialResult(
            code=code, score=40, catalyst_type="unknown",
            freshness="recent", theme_link="", dart_grade=4,
            headline_summary=reason, decay_risk="mid", reasons=[reason],
        )

    def _rule_based_fallback(self, dart: dict, news: dict) -> dict:
        """Gemini 실패 시 규칙 기반 판정."""
        return {
            "freshness": "recent",
            "catalyst_type": "unknown",
            "theme_link": "",
            "summary": news.get("summary", "AI 분석 실패 — 규칙 기반 처리"),
            "decay_risk": "mid",
        }

    def reset_counter(self):
        self._eval_count = 0
