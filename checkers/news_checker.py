"""네이버 뉴스 검색 — '{종목명}' + '{종목명} 특징주' 병행."""

import time
import requests
from config import (
    NAVER_CLIENT_ID, NAVER_CLIENT_SECRET,
    NEWS_SEARCH_COUNT, NEWS_SEARCH_FEATURE, NEWS_MAX_AGE_DAYS,
    API_SLEEP_NAVER, API_TIMEOUT, setup_logging,
)

logger = setup_logging().getChild("news_checker")


def check_news(stock_name: str) -> dict:
    """뉴스 검색 → {items, summary, score, reasons}."""
    items = _search(stock_name, NEWS_SEARCH_COUNT)

    # "특징주" 추가 검색
    feature_items = []
    if NEWS_SEARCH_FEATURE:
        feature_items = _search(f"{stock_name} 특징주", 3)

    all_items = items + feature_items
    # 중복 제거 (제목 기준)
    seen = set()
    unique = []
    for item in all_items:
        title = item.get("title", "")
        if title not in seen:
            seen.add(title)
            unique.append(item)

    summary, score, reasons = _simple_judge(unique)
    return {"items": unique[:NEWS_SEARCH_COUNT + 3],
            "summary": summary, "score": score, "reasons": reasons}


def _search(query: str, count: int) -> list[dict]:
    if not NAVER_CLIENT_ID:
        return []
    try:
        time.sleep(API_SLEEP_NAVER)
        resp = requests.get(
            "https://openapi.naver.com/v1/search/news.json",
            params={"query": query, "display": count, "sort": "date"},
            headers={"X-Naver-Client-Id": NAVER_CLIENT_ID,
                     "X-Naver-Client-Secret": NAVER_CLIENT_SECRET},
            timeout=API_TIMEOUT,
        )
        data = resp.json()
        return data.get("items", [])
    except Exception as e:
        logger.warning(f"네이버 뉴스 실패 '{query}': {e}")
        return []


def _simple_judge(items: list[dict]) -> tuple[str, int, list[str]]:
    """간이 호재/악재 판정 (Gemini 없이)."""
    if not items:
        return "뉴스 없음", 10, ["뉴스 0건"]

    positive = ["수주", "계약", "호재", "급등", "상한가", "흑자", "성장", "수출", "최초"]
    negative = ["하락", "적자", "악재", "손실", "횡령", "감자", "유증", "상폐"]

    pos_count = 0
    neg_count = 0
    for item in items:
        title = item.get("title", "")
        pos_count += sum(1 for kw in positive if kw in title)
        neg_count += sum(1 for kw in negative if kw in title)

    reasons = [f"뉴스 {len(items)}건"]
    if pos_count > neg_count:
        return "호재성 뉴스", min(25, 10 + pos_count * 3), reasons + [f"호재키워드 {pos_count}"]
    elif neg_count > pos_count:
        return "악재성 뉴스", max(0, 10 - neg_count * 3), reasons + [f"악재키워드 {neg_count}"]
    return "중립", 10, reasons
