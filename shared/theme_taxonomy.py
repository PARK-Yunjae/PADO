"""
PADO v2 — 하이브리드 테마 분류 체계
====================================
Level 1: 8개 메가테마 (소스 간 "일치" 판정용)
Level 2: 16개 canon (세분화, stock_mapping 연동)
Level 3: 키워드 (키움 200+개 테마, Gemini 자유텍스트)

핵심:
  - 3소스(키움 API / 거래대금 변화율 / 뉴스)를 canon으로 통일
  - canon 간 비교 시 mega 레벨로 올려서 매칭 확률 향상
  - 정치인 키워드는 political_figures.json에서 동적 로드
"""

import json
from pathlib import Path
from config import REFERENCE_DIR

# ─────────────────────────────────────────────
# Level 2: 16개 canon
# ─────────────────────────────────────────────

CANON_LIST = [
    "반도체", "AI", "2차전지", "원전", "바이오",
    "방산", "조선", "금융", "건설", "게임",
    "자동차", "화학", "철강", "유통", "식품",
    "정치",
]

# ─────────────────────────────────────────────
# Level 1: 8개 메가테마 → canon 매핑
# ─────────────────────────────────────────────

MEGA_THEMES = {
    "반도체_IT":       ["반도체", "AI"],
    "바이오_헬스케어":   ["바이오"],
    "2차전지_에너지":    ["2차전지", "원전"],
    "방산_조선_기계":    ["방산", "조선"],
    "금융_부동산":       ["금융", "건설"],
    "소비재_미디어":     ["게임", "유통", "식품"],
    "소재_화학":        ["화학", "철강", "자동차"],
    "정치_정책":        ["정치"],
}

# canon → mega 역매핑 (자동 생성)
_CANON_TO_MEGA: dict[str, str] = {}
for _mega, _canons in MEGA_THEMES.items():
    for _c in _canons:
        _CANON_TO_MEGA[_c] = _mega

# ─────────────────────────────────────────────
# 키움 테마 → canon (키워드 포함 매칭)
# ─────────────────────────────────────────────

CANON_KEYWORDS = {
    "반도체": [
        "반도체", "HBM", "파운드리", "메모리", "DDR", "NAND",
        "시스템반도체", "팹리스", "EUV", "TSMC", "웨이퍼",
        "패키징", "후공정", "전공정", "SiC",
    ],
    "AI": [
        "AI", "인공지능", "챗GPT", "LLM", "GPU",
        "로봇", "휴머노이드", "자율주행", "딥러닝",
        "클라우드", "데이터센터", "엔비디아", "협동로봇",
        "소프트웨어", "SaaS",
    ],
    "2차전지": [
        "2차전지", "배터리", "양극재", "음극재", "리튬",
        "전고체", "전기차", "충전", "ESS", "LFP",
        "분리막", "전해액", "니켈", "코발트",
    ],
    "원전": [
        "원전", "원자력", "SMR", "소형원자로",
        "터빈", "핵융합", "우라늄",
    ],
    "바이오": [
        "바이오", "제약", "신약", "임상", "헬스케어",
        "의료기기", "진단", "세포치료", "항체", "백신",
        "CMO", "CDMO", "FDA", "비만치료",
    ],
    "방산": [
        "방산", "방위", "무기", "K방산", "군수",
        "우주항공", "드론", "미사일", "위성",
        "레이더", "항공",
    ],
    "조선": [
        "조선", "LNG선", "해양플랜트", "선박",
        "해운", "컨테이너",
    ],
    "금융": [
        "은행", "증권", "보험", "핀테크",
        "배당", "금리", "카드", "캐피탈",
    ],
    "건설": [
        "건설", "인프라", "시멘트", "레미콘",
        "건자재", "부동산", "리츠",
    ],
    "게임": [
        "게임", "메타버스", "엔터", "콘텐츠",
        "IP", "미디어", "플랫폼", "웹툰",
    ],
    "자동차": [
        "자동차", "현대차", "기아", "자동차부품",
        "타이어", "모빌리티",
    ],
    "화학": [
        "화학", "정유", "석유화학", "소재",
        "플라스틱", "페인트", "접착제",
    ],
    "철강": [
        "철강", "비철금속", "희토류", "알루미늄",
        "구리", "아연", "고철",
    ],
    "유통": [
        "유통", "면세점", "이커머스", "홈쇼핑",
        "편의점", "백화점", "소매",
    ],
    "식품": [
        "식품", "음식료", "농업", "비료",
        "사료", "수산", "축산", "음료",
        "제과", "라면", "유제품",
    ],
    "정치": [
        "대선", "총선", "정치", "탄핵",
        "대통령", "정권", "여당", "야당",
    ],
}

# ─────────────────────────────────────────────
# stock_mapping.csv 섹터 → canon
# ─────────────────────────────────────────────
# 실제 값이 한국표준산업분류(KSIC) 세분류명이므로 키워드 매칭 필요.
# 예: "반도체 제조업", "소프트웨어 개발 및 공급업"

SECTOR_CANON_KEYWORDS = {
    "반도체": ["반도체", "전자부품", "마그네틱", "광학 매체",
              "영상 및 음향기기", "전구", "조명장치",
              "절연선", "케이블", "측정", "시험", "정밀기기"],
    "AI": ["소프트웨어", "컴퓨터 프로그래밍", "컴퓨터 및 주변장치",
           "자료처리", "호스팅", "포털", "인터넷 정보",
           "정보 서비스", "통신 및 방송 장비", "전기 통신",
           "텔레비전 방송", "기록매체 복제"],
    "2차전지": ["일차전지", "이차전지"],
    "원전": ["전기업", "증기, 냉·온수", "연료용 가스"],
    "바이오": ["의약품", "기초 의약", "의료용 기기", "의료용품",
              "자연과학 및 공학 연구"],
    "방산": ["무기", "총포탄", "항공기", "우주선", "철도장비"],
    "조선": ["선박", "보트 건조", "해상 운송"],
    "금융": ["금융 지원", "은행 및 저축", "보험업", "보험 및 연금",
            "신탁업", "집합투자", "기타 금융", "재 보험"],
    "건설": ["건물 건설", "토목 건설", "건물설비", "건축기술",
            "엔지니어링", "실내건축", "건축마무리",
            "시멘트", "석회", "플라스터",
            "기반조성", "시설물 축조", "전기 및 통신 공사",
            "부동산"],
    "게임": ["영화, 비디오물", "방송프로그램 제작",
            "오디오물 출판", "원판 녹음",
            "영상·오디오물 제공", "창작 및 예술", "유원지", "오락",
            "스포츠 서비스"],
    "자동차": ["자동차"],
    "화학": ["기초 화학", "기타 화학", "합성고무", "플라스틱 물질",
            "석유 정제", "화학섬유", "비료", "농약", "살균", "살충",
            "플라스틱제품", "고무제품",
            "비내화 요업", "유리 및 유리제품", "기타 비금속 광물"],
    "철강": ["1차 철강", "1차 비철금속", "금속 주조", "기타 금속 가공",
            "구조용 금속", "귀금속", "해체, 선별", "원료 재생"],
    "유통": ["종합 소매", "상품 중개", "상품 종합 도매", "무점포 소매",
            "기타 상품 전문 소매", "기타 생활", "섬유, 의복, 신발",
            "가전제품 및 정보통신장비 소매",
            "여행사", "광고업", "전문디자인",
            "봉제의복", "의복 액세서리", "가죽, 가방", "신발 및 신발",
            "편조의복", "편조원단",
            "방적 및 가공사", "섬유제품 염색",
            "직물직조", "기타 섬유"],
    "식품": ["식품", "곡물가공", "과실, 채소", "도축, 육류",
            "수산물", "동·식물성 유지", "낙농", "사료", "조제식품",
            "알코올음료", "비알코올음료", "떡, 빵", "과자",
            "담배", "음식점", "음·식료품",
            "연료 소매", "작물 재배", "어로"],
}

# ─────────────────────────────────────────────
# 뉴스 THEME_KEYWORDS → canon
# ─────────────────────────────────────────────

NEWS_CANON_MAP = {
    "반도체": "반도체",
    "2차전지": "2차전지",
    "바이오": "바이오",
    "AI": "AI",
    "방산": "방산",
    "원전": "원전",
    "로봇": "AI",
    "정치": "정치",
    "조선": "조선",
    "금융": "금융",
}

# ─────────────────────────────────────────────
# 정치인 동적 키워드 (런타임 로드)
# ─────────────────────────────────────────────

_POLITICAL_KEYWORDS: list[str] = []


def _load_political_keywords():
    """reference/political_figures.json에서 인물명 동적 로드."""
    global _POLITICAL_KEYWORDS
    p = REFERENCE_DIR / "political_figures.json"
    if not p.exists():
        return
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        for fig in data.get("figures", []):
            _POLITICAL_KEYWORDS.append(fig["name"])
            _POLITICAL_KEYWORDS.append(f"{fig['name']}관련주")
        _POLITICAL_KEYWORDS.extend(data.get("active_events", []))
    except Exception:
        pass


_load_political_keywords()


# ─────────────────────────────────────────────
# 정규화 함수
# ─────────────────────────────────────────────

def normalize_kiwoom_theme(thema_nm: str) -> str | None:
    """키움 테마명 → canon. 매칭 안 되면 None."""
    if not thema_nm:
        return None
    name_lower = thema_nm.lower()
    # 정치인 키워드 우선 체크
    for pk in _POLITICAL_KEYWORDS:
        if pk.lower() in name_lower:
            return "정치"
    # 일반 키워드 체크
    for canon, keywords in CANON_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in name_lower:
                return canon
    return None


def normalize_sector(sector: str) -> str | None:
    """stock_mapping 섹터(KSIC 세분류명) → canon. 키워드 매칭."""
    if not sector:
        return None
    sector_lower = sector.lower()
    # 키워드 매칭 (KSIC 세분류명 대응)
    for canon, keywords in SECTOR_CANON_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in sector_lower:
                return canon
    # 일반 기계류 → 조선 (기본)
    if "기계" in sector:
        return "조선"
    # 전기/전동기 → 반도체
    if "전기" in sector or "전동기" in sector:
        return "반도체"
    # 운송 → 조선
    if "운송" in sector or "해운" in sector:
        return "조선"
    # 종이/펄프/나무
    if "종이" in sector or "펄프" in sector or "나무" in sector or "목재" in sector:
        return "화학"
    # 가구/인쇄
    if "가구" in sector or "인쇄" in sector:
        return "화학"
    # 폐기물
    if "폐기물" in sector:
        return "화학"
    return None


def normalize_news_theme(theme_key: str) -> str | None:
    """뉴스 THEME_KEYWORDS 키 → canon."""
    return NEWS_CANON_MAP.get(theme_key)


def canon_to_mega(canon: str) -> str | None:
    """canon → 메가테마. 매핑 없으면 None."""
    return _CANON_TO_MEGA.get(canon)


def resolve_keyword(keyword: str) -> str | None:
    """자유 키워드 → canon. 부분매칭 지원.
    예: "HBM반도체" → "반도체", "바이오시밀러" → "바이오"
    """
    if not keyword:
        return None
    return normalize_kiwoom_theme(keyword)  # 같은 로직


def resolve_to_mega(keyword: str) -> str | None:
    """자유 키워드 → 메가테마 (canon 경유)."""
    canon = resolve_keyword(keyword)
    if canon:
        return canon_to_mega(canon)
    return None


# ─────────────────────────────────────────────
# 3소스 병합
# ─────────────────────────────────────────────

def merge_theme_sources(
    kiwoom_themes: list[str],
    change_sectors: list[str],
    news_themes: list[str],
) -> tuple[int, list[str]]:
    """
    3소스 → 병합 점수(35점 만점) + 최종 leading_themes (canon 기준).

    점수 매트릭스:
      3소스 일치 → 35점
      2소스 일치 → 30점
      1소스 + 집중 → 22점
      1소스만     → 18점
      전부 빈     → 10점
    """
    from collections import Counter

    # 각 소스를 canon으로 변환
    k_canons = []
    for t in kiwoom_themes:
        c = normalize_kiwoom_theme(t)
        if c and c not in k_canons:
            k_canons.append(c)

    c_canons = []
    for s in change_sectors:
        # change_sectors는 market_engine에서 이미 canon으로 변환되어 옴
        # 혹시 원본 섹터가 왔을 경우 normalize 시도
        c = s if s in CANON_LIST else normalize_sector(s)
        if c and c not in c_canons:
            c_canons.append(c)

    n_canons = []
    for t in news_themes:
        c = normalize_news_theme(t)
        if c and c not in n_canons:
            n_canons.append(c)

    active = [s for s in [k_canons, c_canons, n_canons] if s]
    if not active:
        return 10, []

    # 모든 canon을 mega로 올려서 교집합 확인
    all_canons = k_canons + c_canons + n_canons
    mega_counts = Counter()
    canon_to_mega_map = {}
    for c in all_canons:
        mega = canon_to_mega(c)
        if mega:
            mega_counts[mega] += 1
            if mega not in canon_to_mega_map:
                canon_to_mega_map[mega] = c

    # 3소스 일치 (mega 레벨에서 3회 이상)
    triple = [m for m, cnt in mega_counts.most_common() if cnt >= 3]
    if triple:
        themes = [canon_to_mega_map[m] for m in triple[:3]]
        return 35, themes

    # 2소스 일치
    double = [m for m, cnt in mega_counts.most_common() if cnt >= 2]
    if double:
        themes = [canon_to_mega_map[m] for m in double[:3]]
        return 30, themes

    # 1소스만
    if len(active) == 1:
        canons = active[0][:3]
        # 집중도 체크: 거래대금 변화율에서 특정 canon 비중 높으면 보너스
        if c_canons and len(c_canons) >= 1:
            return 22, canons
        return 18, canons

    # 여러 소스 있지만 mega 겹침 없음 → 변화율(소스2) 우선
    priority = c_canons or k_canons or n_canons
    return 18, priority[:3]


# ─────────────────────────────────────────────
# 매칭 유틸
# ─────────────────────────────────────────────

def themes_match(themes_a: list[str], themes_b: list[str]) -> bool:
    """두 canon 리스트가 mega 레벨에서 겹치는지."""
    megas_a = {canon_to_mega(c) for c in themes_a if canon_to_mega(c)}
    megas_b = {canon_to_mega(c) for c in themes_b if canon_to_mega(c)}
    return bool(megas_a & megas_b)


def theme_match_score(
    material_theme: str,
    market_themes: list[str],
    stock_sector: str,
    stock_themes: list[str],
) -> int:
    """종목-시황 테마 매칭 보너스 (intersection.py용).

    Returns:
        15: 재료 theme_link가 시황과 mega 일치
        8: 종목 섹터/테마가 시황과 mega 일치
        0: 불일치
    """
    if not market_themes:
        return 0

    market_megas = set()
    for t in market_themes:
        m = canon_to_mega(t)
        if m:
            market_megas.add(m)

    # 재료 theme_link 매칭 (15점)
    if material_theme:
        mat_canon = resolve_keyword(material_theme)
        if mat_canon:
            mat_mega = canon_to_mega(mat_canon)
            if mat_mega and mat_mega in market_megas:
                return 15

    # 종목 섹터 매칭 (8점)
    if stock_sector:
        sec_canon = normalize_sector(stock_sector)
        if sec_canon:
            sec_mega = canon_to_mega(sec_canon)
            if sec_mega and sec_mega in market_megas:
                return 8

    # 종목 themes 매칭 (8점)
    for t in stock_themes:
        t_mega = canon_to_mega(t)
        if t_mega and t_mega in market_megas:
            return 8

    return 0


# ─────────────────────────────────────────────
# Gemini 프롬프트 타입 매핑 (기능3용)
# ─────────────────────────────────────────────

_CANON_TO_PROMPT = {
    "바이오": "bio",
    "반도체": "tech",
    "AI": "tech",
    "2차전지": "policy",
    "원전": "policy",
    "금융": "finance",
    "건설": "finance",
    "방산": "defense",
    "조선": "defense",
    "정치": "political",
}


def get_prompt_type(sector: str) -> str:
    """종목 섹터 → Gemini 프롬프트 타입."""
    canon = normalize_sector(sector)
    if canon:
        return _CANON_TO_PROMPT.get(canon, "general")
    return "general"
