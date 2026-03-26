"""
PADO v3 — 뉴스 인텔리전스 엔진
=================================
Layer 0: Google News RSS + 네이버 확장 수집
Layer 1: 활성 단어 추출 + 종목 매칭
Layer 2: 델타 감지 ("없다가 생긴 것") + Gemini 키워드→종목 추론
Layer 3: 시황·재료 채널 공급

Google News RSS: API 키 불필요, 호출 제한 없음, 완전 무료.
"""

import re
import time
import json
import requests
import feedparser
from datetime import datetime, timedelta
from collections import Counter
from html import unescape

from config import (
    NAVER_CLIENT_ID, NAVER_CLIENT_SECRET,
    GEMINI_API_KEY, GEMINI_MODEL,
    API_SLEEP_NAVER, API_SLEEP_GEMINI, API_TIMEOUT,
    MAPPING_CSV,
    setup_logging,
)
from shared import storage

logger = setup_logging().getChild("news_intel")


# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

# ── Google News RSS: 광범위 수집 (제목 기반 델타 감지용) ──
# API 키 불필요, 호출 제한 없음, 무료
# 단점: summary에 기사 본문 없음 (HTML 링크만)
GNEWS_KO_QUERIES = {
    "market":    ["코스피", "코스닥", "증시 급등", "상한가 종목", "거래대금 역대",
                  "외국인 매수", "기관 매수", "프로그램 매매"],
    "sector":    ["반도체 HBM", "2차전지 배터리", "바이오 신약", "AI 인공지능",
                  "방산 수출", "원전 SMR", "로봇 휴머노이드", "조선 수주",
                  "건설 인프라", "게임 신작", "자동차 전기차", "화학 소재",
                  "엔터 K-POP", "금융 보험", "통신 5G", "우주항공 위성"],
    "macro":     ["금리 인하", "환율 원달러", "국제유가", "무역수지 수출",
                  "경기침체", "생산자물가", "소비자물가 CPI", "고용지표",
                  "한국은행 기준금리"],
    "political": ["대선 후보", "국회 법안", "규제 완화", "정책 발표",
                  "미중 관세", "중동 전쟁", "트럼프 관세", "북한 미사일",
                  "대통령 긴급", "탄핵 정국"],
    "corporate": ["공시 상장폐지", "횡령 배임", "CB 전환사채", "유상증자 결정",
                  "자사주 매입", "대주주 지분 변동", "경영권 분쟁"],
    "social":    ["부동산 아파트", "전력 에너지", "태양광 풍력",
                  "수소경제", "탄소중립"],
}

GNEWS_EN_QUERIES = {
    "us_politics": ["Trump tariff", "Trump executive order", "US China trade",
                    "US sanctions", "White House economy", "Congress bill",
                    "Trump Korea", "trade war escalation"],
    "fed_macro":   ["Federal Reserve rate", "US inflation CPI", "US jobs report",
                    "Treasury yields", "US GDP growth", "recession risk",
                    "unemployment claims", "consumer confidence"],
    "geopolitics": ["Middle East conflict", "Taiwan strait", "Russia Ukraine",
                    "OPEC oil output", "Iran nuclear", "North Korea missile",
                    "Red Sea shipping", "China military"],
    "tech_semi":   ["NVIDIA earnings", "semiconductor export", "AI chip",
                    "TSMC foundry", "Samsung HBM", "Apple supply chain",
                    "AMD Intel", "memory chip demand", "AI datacenter"],
    "markets":     ["S&P 500 rally", "Nasdaq futures", "VIX volatility",
                    "dollar index DXY", "Bitcoin crypto", "emerging markets",
                    "Japan Nikkei", "Europe DAX", "bond yield surge"],
    "commodities": ["oil price crude", "gold price surge", "copper demand",
                    "lithium battery", "natural gas Europe"],
}

# ── 네이버: 정밀 수집 (본문 snippet 포함, Gemini 분석용) ──
# Google RSS에 없는 것 = 기사 본문. 핵심 키워드만 소수 정밀.
# 10개 × 10건 = 100건, API 10콜 (일 25,000 한도 대비 여유)
NAVER_PRECISION_QUERIES = [
    "반도체",           # 테마 대장
    "2차전지",
    "바이오 임상",
    "AI 인공지능",
    "방산 수주",
    "코스피 급등",       # 시장 전체 맥락
    "금리 통화정책",      # 거시
    "대선 정책",         # 정치
    "원전 SMR",
    "조선 해운",
    "로봇 자동화",       # v5 추가
    "게임 신작 출시",
    "트럼프 관세",       # 해외 이슈 국내 반응
    "공매도 과열",       # 수급
    "상한가 테마",       # 주도주 탐색
]

# 불용어 (제목에서 제거할 더미 단어)
STOPWORDS_KO = {
    "것", "등", "위", "수", "중", "및", "더", "때", "만", "곳", "약",
    "오늘", "내일", "어제", "관련", "전망", "분석", "종합", "속보", "단독",
    "기자", "뉴스", "보도", "취재", "리포트", "인터뷰", "사진", "영상",
    "대한", "통한", "따른", "위한", "되는", "있는", "하는", "되어",
    "그리고", "하지만", "그러나", "또한", "이에", "한편",
    # v3: 테스트에서 발견된 노이즈
    "연합뉴스", "뉴시스", "이데일리", "머니투데이", "한국경제", "매일경제",
    "서울경제", "아시아경제", "헤럴드경제", "파이낸셜뉴스",
    "네이버", "다음", "블로그", "카페", "네이트",
    "했다", "된다", "한다", "있다", "없다", "같다", "라며", "이라고",
    "지난해", "올해", "내년", "전년", "대비", "이상", "이하", "이후",
    "맞은", "담은", "사들였는데", "수익률은", "코스피보다", "깨졌다",
    "돌파", "마이너스", "플러스", "상승", "하락", "급등", "급락",
    "글로벌", "시장", "투자", "가능", "예상", "발표", "증가", "감소",
    # v4.1: 부상 키워드에서 발견된 잡음
    "보유한", "기대감에", "회복으로", "수요에", "대응하고",
    "확대에", "가능성에", "영향으로", "방침에", "변동에",
    "나타나며", "기록하며", "보이면서", "나오면서", "전해지면서",
    "것으로", "까지", "부터", "에서", "으로", "에게", "처럼",
    "코스피", "코스닥", "종목", "주가", "거래",
    "전문가", "애널리스트", "증권사", "리서치",
    "지속", "유지", "강화", "확대", "추진", "계획", "검토", "논의",
    "중단", "재개", "변경", "조정", "완료", "마감",
    # v5: 추가 일반 단어
    "결정", "발생", "진행", "예정", "실시", "실행", "시행", "개시",
    "선정", "선발", "도입", "출범", "체결", "합의", "승인", "허가",
    "대상", "기준", "목표", "규모", "수준", "현재", "최근", "전일",
    "매매", "매수", "매도", "거래량", "시총", "영업", "분기",
    "위원회", "기획", "정부", "행정", "산업", "분야", "대한",
}
STOPWORDS_EN = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "has", "have", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "can", "this", "that", "these", "those",
    "it", "its", "he", "she", "they", "we", "you", "i", "my", "your",
    "his", "her", "their", "our", "said", "says", "also", "new", "more",
    "how", "what", "when", "where", "who", "why", "which", "than", "not",
    "about", "after", "before", "into", "over", "just", "like", "up",
    # 미디어/출처
    "according", "report", "reports", "reuters", "bloomberg", "cnbc",
    "year", "week", "month", "today", "first",
    "times", "post", "journal", "review", "associated", "press",
    # URL/RSS 잔해
    "com", "www", "http", "https", "html", "news", "article", "articles",
    "net", "org", "rss", "feed", "google", "naver", "daum",
    # 일반 동사/부사/전치사
    "says", "told", "via", "per", "still", "now", "get", "got", "make",
    "most", "some", "other", "many", "much", "well", "back", "down",
    "out", "all", "been", "being", "then", "here", "there", "very",
    "its", "two", "three", "one", "last", "next", "each", "both",
    "amid", "despite", "warns", "set", "sets", "plan", "plans",
    "take", "takes", "move", "moves", "push", "sees", "face",
    "keep", "call", "calls", "hit", "hits", "eyes", "deal",
    # 단독 출현 시 무의미한 지명/일반명사
    "north", "south", "east", "west", "european", "american", "asian",
    "iran", "iraq", "china", "korea", "japan", "india", "europe",
    "oil", "gold", "gas", "energy", "trade", "economy",
    "state", "major", "key", "top", "big", "large", "early",
    "rate", "rates", "tax", "debt", "war", "crisis", "risk",
    # 금융 일반
    "global", "market", "markets", "stock", "stocks", "share", "shares",
    "index", "data", "growth", "rise", "fall", "high", "low",
    "company", "companies", "investors", "investor", "trading",
    "billion", "million", "percent", "price", "prices",
    "bank", "fund", "funds", "bond", "bonds", "rally", "drop",
}

# Google RSS URL 패턴 노이즈 (활성 단어에서 제거)
URL_NOISE = {"DAUM", "NET", "COM", "WWW", "HTTP", "HTTPS", "NAVER", "KR",
             "NEWS", "GOOGLE", "RSS", "ARTICLES"}

# 종목명 사전 (lazy load)
_stock_dict = None


def _load_stock_dict() -> dict:
    """stock_mapping.csv → {종목명: 코드} 사전 로드."""
    global _stock_dict
    if _stock_dict is not None:
        return _stock_dict

    _stock_dict = {}
    try:
        import pandas as pd
        df = pd.read_csv(MAPPING_CSV, dtype={"code": str}, encoding="utf-8-sig")
        df["code"] = df["code"].str.zfill(6)
        for _, row in df.iterrows():
            name = str(row.get("name", "")).strip()
            code = row["code"]
            if name and len(name) >= 2:
                _stock_dict[name] = code
    except Exception as e:
        logger.warning(f"종목 사전 로드 실패: {e}")

    logger.info(f"종목 사전: {len(_stock_dict)}개 로드")
    return _stock_dict


# ─────────────────────────────────────────────
# Layer 0: 수집
# ─────────────────────────────────────────────

def collect_google_news_rss(date: str) -> int:
    """Google News RSS 수집 → news_v2 저장. 반환: 저장 건수."""
    all_items = []
    seen_titles = set()

    # 한국어 뉴스
    for category, queries in GNEWS_KO_QUERIES.items():
        for query in queries:
            try:
                items = _fetch_rss_google(query, lang="ko")
                for item in items:
                    title = _clean_html(item.get("title", ""))
                    if title and title not in seen_titles and len(title) > 10:
                        seen_titles.add(title)
                        all_items.append(_build_news_row(
                            date, "google_rss", category, query,
                            title, item.get("summary", ""),
                            item.get("link", ""), item.get("published", ""),
                            _extract_publisher(item), "ko",
                        ))
            except Exception as e:
                logger.debug(f"RSS 수집 실패 '{query}': {e}")
            time.sleep(0.3)

    # 영어 뉴스 (외신)
    for category, queries in GNEWS_EN_QUERIES.items():
        for query in queries:
            try:
                items = _fetch_rss_google(query, lang="en")
                for item in items:
                    title = _clean_html(item.get("title", ""))
                    if title and title not in seen_titles and len(title) > 10:
                        seen_titles.add(title)
                        all_items.append(_build_news_row(
                            date, "google_rss", category, query,
                            title, item.get("summary", ""),
                            item.get("link", ""), item.get("published", ""),
                            _extract_publisher(item), "en",
                        ))
            except Exception as e:
                logger.debug(f"RSS EN 수집 실패 '{query}': {e}")
            time.sleep(0.3)

    if all_items:
        storage.save_news_v2_batch(all_items)
        logger.info(f"Google RSS 수집: {len(all_items)}건 (KO+EN)")

    return len(all_items)


def collect_naver_precision(date: str) -> int:
    """네이버 뉴스 API 정밀 수집 → news_v2 저장.

    Google RSS에 없는 기사 본문(description)이 핵심.
    핵심 키워드 10개 × 10건 = API 10콜만 사용.
    """
    if not NAVER_CLIENT_ID:
        return 0

    all_items = []
    seen_titles = set()

    for query in NAVER_PRECISION_QUERIES:
        try:
            items = _search_naver(query, 10)
            for item in items:
                title = _clean_html(item.get("title", ""))
                if title and title not in seen_titles:
                    seen_titles.add(title)
                    # 네이버의 강점: description에 기사 본문 앞부분 포함
                    desc = _clean_html(item.get("description", ""))
                    all_items.append(_build_news_row(
                        date, "naver", "precision", query,
                        title, desc[:300],
                        item.get("link", ""), item.get("pubDate", ""),
                        _extract_domain(item.get("originallink", "")), "ko",
                    ))
        except Exception as e:
            logger.debug(f"네이버 수집 실패 '{query}': {e}")
        time.sleep(API_SLEEP_NAVER)

    if all_items:
        storage.save_news_v2_batch(all_items)
        logger.info(f"네이버 정밀 수집: {len(all_items)}건 (본문 snippet 포함)")

    return len(all_items)


def _fetch_rss_google(query: str, lang: str = "ko", max_items: int = 15) -> list[dict]:
    """Google News RSS 조회."""
    if lang == "ko":
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=ko&gl=KR&ceid=KR:ko"
    else:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=en&gl=US&ceid=US:en"

    feed = feedparser.parse(url)
    return feed.entries[:max_items]


def _search_naver(query: str, count: int) -> list[dict]:
    """네이버 뉴스 검색 API."""
    resp = requests.get(
        "https://openapi.naver.com/v1/search/news.json",
        params={"query": query, "display": count, "sort": "date"},
        headers={
            "X-Naver-Client-Id": NAVER_CLIENT_ID,
            "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        },
        timeout=API_TIMEOUT,
    )
    return resp.json().get("items", [])


def _build_news_row(date, source, category, query, title, snippet,
                    link, pub_date, publisher, lang) -> dict:
    """뉴스 행 구성 + 활성 단어 추출 + 종목 매칭."""
    active = extract_active_words(title, snippet, lang)
    stocks = match_stock_mentions(active)

    return {
        "collect_date": date,
        "source": source,
        "category": category,
        "query": query,
        "title": title,
        "first_sentence": snippet[:300] if snippet else "",
        "snippet": snippet[:200] if snippet else "",
        "link": link,
        "pub_date": pub_date,
        "publisher": publisher,
        "active_words": json.dumps(active, ensure_ascii=False),
        "stock_mentions": json.dumps(stocks, ensure_ascii=False),
        "lang": lang,
    }


# ─────────────────────────────────────────────
# Layer 1: 전처리
# ─────────────────────────────────────────────

def extract_active_words(title: str, snippet: str = "", lang: str = "ko") -> list[str]:
    """제목 + 스니펫에서 활성 단어 추출 (더미 제거)."""
    text = f"{title} {snippet}"

    if lang == "ko":
        return _extract_ko_words(text)
    else:
        return _extract_en_words(text)


def _extract_ko_words(text: str) -> list[str]:
    """한국어 활성 단어 추출 (정규식 기반, 형태소 분석기 없이)."""
    # HTML 태그 제거
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)

    words = []

    # 1) 영문 단어/약어 (HBM, AI, FDA, NVIDIA 등)
    eng_words = re.findall(r"[A-Za-z][A-Za-z0-9]{1,15}", text)
    for w in eng_words:
        upper = w.upper()
        if len(w) >= 2 and w.lower() not in STOPWORDS_EN and upper not in URL_NOISE:
            words.append(upper if len(w) <= 5 else w)

    # 2) 한글 2~6글자 단어 추출 (띄어쓰기 기준)
    ko_tokens = re.findall(r"[가-힣]{2,8}", text)
    for w in ko_tokens:
        if w not in STOPWORDS_KO and len(w) >= 2:
            words.append(w)

    # 3) 숫자+단위 (1조, 100억 등) — 단독 퍼센트는 노이즈라 제외
    nums = re.findall(r"\d+(?:\.\d+)?[조억만달러원]", text)
    words.extend(nums)

    # 중복 제거 (순서 유지)
    seen = set()
    unique = []
    for w in words:
        if w not in seen:
            seen.add(w)
            unique.append(w)

    return unique[:30]  # 최대 30개


def _extract_en_words(text: str) -> list[str]:
    """영문 활성 단어 추출."""
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)

    words = []
    tokens = re.findall(r"[A-Za-z][A-Za-z'-]{1,20}", text)
    for w in tokens:
        low = w.lower()
        if low not in STOPWORDS_EN and len(w) >= 3:
            words.append(w)

    # 숫자+단위
    nums = re.findall(r"\$?\d+(?:\.\d+)?[BMKbmk%]?", text)
    words.extend([n for n in nums if len(n) >= 2])

    seen = set()
    unique = []
    for w in words:
        key = w.lower()
        if key not in seen:
            seen.add(key)
            unique.append(w)

    return unique[:30]


def match_stock_mentions(active_words: list[str]) -> list[dict]:
    """활성 단어에서 종목명 직접 매칭. 간접 연결은 Gemini가 담당."""
    stock_dict = _load_stock_dict()
    if not stock_dict:
        return []

    matches = []
    seen_codes = set()

    for word in active_words:
        if word in stock_dict:
            code = stock_dict[word]
            if code not in seen_codes:
                seen_codes.add(code)
                matches.append({"code": code, "name": word})

    return matches


# ─────────────────────────────────────────────
# Layer 2: 분석
# ─────────────────────────────────────────────

def detect_emerging_topics(date: str, lookback: int = 7) -> list[dict]:
    """최근 N일 대비 오늘 급부상 단어 감지 — '없다가 생긴 것'.

    Returns: [{"word": str, "today": int, "avg": float, "delta": str, "titles": [...]}]
    """
    from datetime import date as dt_date

    # 오늘 뉴스
    today_news = storage.get_news_v2_by_date(date)
    if not today_news:
        return []

    # 오늘 단어 빈도
    today_counts = Counter()
    word_titles = {}  # word → [관련 제목 목록]
    for row in today_news:
        words = json.loads(row.get("active_words", "[]"))
        title = row.get("title", "")
        for w in words:
            today_counts[w] += 1
            if w not in word_titles:
                word_titles[w] = []
            if len(word_titles[w]) < 3:
                word_titles[w].append(title)

    # 과거 N일 단어 빈도
    past_counts = Counter()
    past_days = 0
    try:
        target = datetime.strptime(date, "%Y-%m-%d")
        for i in range(1, lookback + 1):
            past_date = (target - timedelta(days=i)).strftime("%Y-%m-%d")
            past_news = storage.get_news_v2_by_date(past_date)
            if past_news:
                past_days += 1
                for row in past_news:
                    words = json.loads(row.get("active_words", "[]"))
                    for w in words:
                        past_counts[w] += 1
    except Exception:
        pass

    if past_days == 0:
        # 첫날은 모든 게 "신규"이므로 빈도 5 이상만
        return [
            {"word": w, "today": c, "avg": 0, "delta": "NEW",
             "titles": word_titles.get(w, [])}
            for w, c in today_counts.most_common(20) if c >= 5
        ]

    # 델타 계산
    avg_divisor = max(past_days, 1)
    emerging = []

    for word, today_count in today_counts.items():
        if today_count < 3:
            continue

        avg_past = past_counts.get(word, 0) / avg_divisor

        if avg_past == 0 and today_count >= 3:
            # ★ "없다가 생긴 것"
            emerging.append({
                "word": word, "today": today_count,
                "avg": 0, "delta": "NEW",
                "titles": word_titles.get(word, []),
            })
        elif avg_past > 0:
            delta = (today_count - avg_past) / avg_past
            if delta >= 3.0:
                emerging.append({
                    "word": word, "today": today_count,
                    "avg": round(avg_past, 1), "delta": round(delta, 1),
                    "titles": word_titles.get(word, []),
                })

    # v4.1: 최종 후처리 — 잡음 제거
    def _is_noise(word: str) -> bool:
        if re.match(r"^\d+%?$", word):        # 순수 숫자/퍼센트
            return True
        if len(word) <= 1:                      # 1글자
            return True
        if word in STOPWORDS_KO:                # 불용어 재체크
            return True
        # 2글자 한글 중 조사/어미형
        if re.match(r"^[가-힣]{2}$", word):
            if word.endswith(("에", "은", "는", "이", "가", "를", "의", "도", "로")):
                return True
        return False

    emerging = [e for e in emerging if not _is_noise(e["word"])]

    # 델타 크기순 정렬
    emerging.sort(key=lambda x: x["today"], reverse=True)
    return emerging[:15]


def gemini_infer_stock_impact(emerging_topics: list[dict], date: str) -> dict | None:
    """부상 키워드 → Gemini가 한국 시장 영향 추론.

    Returns: {
        "emerging_themes": [{"keyword": str, "chain": str, "sectors": [...], "stocks": [...]}],
        "market_mood": str,
        "risk_alerts": [str],
    }
    """
    if not GEMINI_API_KEY or not emerging_topics:
        return None

    # 프롬프트 구성
    topics_text = ""
    for t in emerging_topics[:10]:
        delta_str = t["delta"] if isinstance(t["delta"], str) else f"+{t['delta']}배"
        titles = " / ".join(t["titles"][:2])
        topics_text += f"- {t['word']} (오늘 {t['today']}건, 변화: {delta_str}): {titles}\n"

    prompt = f"""당신은 한국 주식시장 전문 애널리스트입니다.

오늘({date}) 뉴스에서 갑자기 부상한 키워드 목록입니다:

{topics_text}

각 키워드에 대해:
1. 이 이슈가 한국 주식시장의 어떤 업종/종목에 수혜 또는 피해를 줄 수 있는지 추론
2. 연결 고리를 설명 (예: "호르무즈→원유수송위기→유조선수요→조선해운")
3. 구체적인 한국 상장 종목명을 제시

반드시 아래 JSON 형식으로만 응답:
{{
  "emerging_themes": [
    {{
      "keyword": "키워드",
      "chain": "연결 고리 한 줄",
      "beneficiary_sectors": ["수혜 업종"],
      "risk_sectors": ["피해 업종"],
      "stocks": ["종목명1", "종목명2"],
      "confidence": "high|mid|low"
    }}
  ],
  "market_mood": "bullish|bearish|mixed|neutral",
  "risk_alerts": ["위험 경고 한 줄"]
}}"""

    try:
        time.sleep(API_SLEEP_GEMINI)
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            params={"key": GEMINI_API_KEY},
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=API_TIMEOUT * 3,
        )
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]

        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0]

        result = json.loads(cleaned)
        logger.info(f"Gemini 키워드→종목 추론: {len(result.get('emerging_themes', []))}건")
        return result

    except Exception as e:
        logger.warning(f"Gemini 추론 실패: {e}")
        return None


# ─────────────────────────────────────────────
# Layer 3: 공급 함수 (시황·재료 채널용)
# ─────────────────────────────────────────────

def get_news_themes_for_market(date: str) -> tuple[list[str], list[dict]]:
    """시황 채널용: 오늘 뉴스 기반 주도테마 + 부상 키워드.

    Returns: (leading_themes: list[str], emerging: list[dict])
    """
    # 1) 뉴스 v2 기반 카테고리별 단어 빈도
    news = storage.get_news_v2_by_date(date)
    if not news:
        # 폴백: 기존 news_daily
        return [], []

    # 테마 키워드 매칭 (기존 방식 + 확장)
    THEME_KEYWORDS = {
        "반도체": ["반도체", "HBM", "파운드리", "메모리", "DRAM", "NAND", "semiconductor"],
        "2차전지": ["2차전지", "배터리", "양극재", "리튬", "전기차", "battery"],
        "바이오": ["바이오", "임상", "신약", "FDA", "제약"],
        "AI": ["AI", "인공지능", "GPU", "엔비디아", "딥러닝", "NVIDIA"],
        "방산": ["방산", "방위", "무기", "K방산", "수출"],
        "원전": ["원전", "SMR", "소형원자로", "원자력"],
        "로봇": ["로봇", "휴머노이드", "자동화", "협동로봇"],
        "정치": ["대선", "총선", "정책", "규제", "탄핵"],
        "조선": ["조선", "해운", "LNG", "선박", "수주"],
        "금융": ["금리", "은행", "금융", "배당", "증권"],
        "건설": ["건설", "인프라", "토목", "아파트"],
        "게임": ["게임", "신작", "출시", "매출"],
    }

    counts = {}
    for theme, keywords in THEME_KEYWORDS.items():
        cnt = 0
        for row in news:
            title = row.get("title", "")
            snippet = row.get("first_sentence", "") or row.get("snippet", "")
            text = f"{title} {snippet}"
            if any(kw in text for kw in keywords):
                cnt += 1
        if cnt >= 3:
            counts[theme] = cnt

    leading = [t for t, _ in sorted(counts.items(), key=lambda x: -x[1])[:3]]

    # 2) 델타 감지
    emerging = detect_emerging_topics(date)

    return leading, emerging


def get_related_news_for_stock(code: str, name: str, days: int = 3) -> list[dict]:
    """재료 채널용: 특정 종목 관련 뉴스 모음.

    stock_mentions에 해당 종목이 있거나, 제목에 종목명이 포함된 뉴스.
    네이버 소스(본문 snippet 포함)를 우선 정렬.
    """
    from datetime import date as dt_date

    today = datetime.now().strftime("%Y-%m-%d")
    results = []

    for i in range(days):
        d = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        news = storage.get_news_v2_by_date(d)
        for row in news:
            # 종목명이 제목에 직접 등장
            if name in row.get("title", ""):
                results.append(row)
                continue
            # stock_mentions에 포함
            try:
                mentions = json.loads(row.get("stock_mentions", "[]"))
                if any(m.get("code") == code for m in mentions):
                    results.append(row)
            except Exception:
                pass

    # 네이버 소스(본문 snippet 있음) 우선 정렬
    results.sort(key=lambda r: (0 if r.get("source") == "naver" else 1))
    return results[:10]


def get_emerging_for_stock(code: str, name: str, sector: str) -> list[str]:
    """특정 종목의 섹터와 관련된 부상 키워드 추출."""
    today = datetime.now().strftime("%Y-%m-%d")
    emerging = detect_emerging_topics(today)

    # 종목명이나 섹터 관련 키워드 필터
    relevant = []
    sector_words = set(sector.split()) if sector else set()

    for topic in emerging:
        word = topic["word"]
        titles = " ".join(topic.get("titles", []))

        if name in titles or word in name:
            relevant.append(f"{word}(뉴스 {topic['today']}건, {topic['delta']})")
        elif sector_words and any(sw in titles for sw in sector_words):
            relevant.append(f"{word}(섹터 연관, {topic['today']}건)")

    return relevant[:5]


# ─────────────────────────────────────────────
# 통합 실행
# ─────────────────────────────────────────────

def run_news_collection(date: str) -> dict:
    """전체 뉴스 수집 실행. main.py에서 호출."""
    stats = {"google_rss": 0, "naver": 0, "total": 0}

    # Google News RSS
    try:
        stats["google_rss"] = collect_google_news_rss(date)
    except Exception as e:
        logger.error(f"Google RSS 수집 실패: {e}")

    # 네이버 정밀 (본문 snippet 포함)
    try:
        stats["naver"] = collect_naver_precision(date)
    except Exception as e:
        logger.error(f"네이버 정밀 수집 실패: {e}")

    stats["total"] = stats["google_rss"] + stats["naver"]
    logger.info(f"뉴스 수집 완료: RSS {stats['google_rss']} + 네이버 {stats['naver']} = {stats['total']}건")

    return stats


def run_news_analysis(date: str) -> dict | None:
    """뉴스 분석 실행 (델타 감지 + Gemini 추론). 파이프라인 ⑬에서 호출."""
    emerging = detect_emerging_topics(date)

    if not emerging:
        logger.info("부상 키워드 없음 — Gemini 스킵")
        return None

    logger.info(f"부상 키워드 {len(emerging)}건: {[e['word'] for e in emerging[:5]]}")

    # Gemini 추론
    result = gemini_infer_stock_impact(emerging, date)

    # DB 저장 (나중에 과거 연결에 사용)
    if result:
        try:
            storage.save_news_analysis(date, {
                "emerging": emerging,
                "gemini_result": result,
            })
        except Exception as e:
            logger.debug(f"분석 결과 저장 실패: {e}")

    return result


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────

def _clean_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    # Google RSS에서 " - 언론사명" 제거
    if " - " in text:
        text = text.rsplit(" - ", 1)[0]
    return text.strip()


def _extract_publisher(entry: dict) -> str:
    """RSS entry에서 언론사명 추출."""
    # Google News RSS: source.title 또는 제목 끝 " - 언론사"
    source = entry.get("source", {})
    if isinstance(source, dict) and source.get("title"):
        return source["title"]
    title = entry.get("title", "")
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return ""


def _extract_domain(url: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return ""
