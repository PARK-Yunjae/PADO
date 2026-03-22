"""
PADO v2 — 뉴스 매일 DB 축적
=============================
8개 키워드 × 30건 = 240건 → 중복 제거 → 150~200건 저장.
파이프라인 ⑫번 슬롯. 네이버 API 8콜 (일 25,000 무료).
"""

import time
import requests
from config import (
    NAVER_CLIENT_ID, NAVER_CLIENT_SECRET,
    NEWS_COLLECT_QUERIES, NEWS_COLLECT_PER_QUERY,
    API_SLEEP_NAVER, API_TIMEOUT,
    setup_logging,
)
from shared import storage

logger = setup_logging().getChild("news_collector")


def collect_daily_news(date: str) -> int:
    """오늘자 뉴스 수집 → DB 저장 → 저장 건수 반환."""
    if not NAVER_CLIENT_ID:
        logger.warning("네이버 API 키 없음 — 뉴스 수집 스킵")
        return 0

    queries = NEWS_COLLECT_QUERIES
    per_query = NEWS_COLLECT_PER_QUERY
    all_items = []
    seen_titles = set()

    for query in queries:
        try:
            items = _search_naver(query, per_query)
            for item in items:
                title = _clean_title(item.get("title", ""))
                if title and title not in seen_titles:
                    seen_titles.add(title)
                    all_items.append({
                        "collect_date": date,
                        "title": title,
                        "snippet": item.get("description", "")[:200],
                        "source": _extract_source(item.get("originallink", "")),
                        "pub_date": item.get("pubDate", ""),
                        "link": item.get("link", ""),
                    })
        except Exception as e:
            logger.warning(f"뉴스 수집 실패 '{query}': {e}")

    if all_items:
        storage.save_news_batch(all_items)
        logger.info(f"뉴스 수집 완료: {len(all_items)}건 (쿼리 {len(queries)}개)")

    return len(all_items)


def _search_naver(query: str, count: int) -> list[dict]:
    """네이버 뉴스 검색 API."""
    time.sleep(API_SLEEP_NAVER)
    resp = requests.get(
        "https://openapi.naver.com/v1/search/news.json",
        params={"query": query, "display": count, "sort": "date"},
        headers={
            "X-Naver-Client-Id": NAVER_CLIENT_ID,
            "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        },
        timeout=API_TIMEOUT,
    )
    data = resp.json()
    return data.get("items", [])


def _clean_title(title: str) -> str:
    """HTML 태그 제거."""
    return title.replace("<b>", "").replace("</b>", "").strip()


def _extract_source(url: str) -> str:
    """URL에서 도메인 추출."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""
