"""Gemini AI 재료 분석 — v2 섹터별 프롬프트."""

import json
import time
import requests
from config import GEMINI_API_KEY, GEMINI_MODEL, API_SLEEP_GEMINI, API_TIMEOUT, AMPLIFIER_KEYWORDS, setup_logging

logger = setup_logging().getChild("ai_analyzer")

# ─────────────────────────────────────────────
# 공통 출력 포맷 (모든 프롬프트 끝에 삽입)
# ─────────────────────────────────────────────

_OUTPUT_FORMAT = """
반드시 아래 JSON 형식으로만 응답 (다른 텍스트 금지):
{{
  "verdict": "bullish|bearish|bearish_gone|neutral",
  "freshness": "first_seen|recent|stale",
  "catalyst_type": "contract|policy|earnings|theme|relief|unknown",
  "theme_link": "다음 중 하나: 반도체, AI, 2차전지, 원전, 바이오, 방산, 조선, 금융, 건설, 게임, 자동차, 화학, 철강, 유통, 식품, 정치, 기타",
  "summary": "한 줄 요약 (30자 이내)",
  "decay_risk": "high|mid|low",
  "amplifier_hit": false
}}

판단 기준:
- freshness: 이 뉴스가 처음 나온 것이면 first_seen, 며칠째면 recent, 1주 이상이면 stale
- bearish_gone: 과거에 악재가 있었으나 현재 해소된 상태
- amplifier_hit: 뉴스에 "세계 최초", "1위", "삼성전자", "대규모 수주" 등 기대감 증폭 키워드 포함 여부
- theme_link: 위 16개 중 가장 가까운 하나를 반드시 선택
"""

# ─────────────────────────────────────────────
# 섹터별 프롬프트 헤더
# ─────────────────────────────────────────────

_PROMPT_BIO = """당신은 한국 바이오/제약 전문 주식 재료 분석가입니다.
판단 중점:
- 임상 단계 정확히 파악: 전임상 < P1 < P2 < P3 < NDA/BLA
- P3 성공·FDA 승인 → catalyst_type="contract", freshness="first_seen"
- 기술수출 계약금+마일스톤 1000억+ → amplifier_hit=true
- "완전관해", "세계 최초", "글로벌 빅파마 제휴" → amplifier_hit=true
- 바이오 CB/유상증자는 흔하지만 대규모는 bearish 신호
- 반사이익(경쟁사 임상 실패)도 고려
"""

_PROMPT_TECH = """당신은 한국 반도체/IT/AI 전문 주식 재료 분석가입니다.
판단 중점:
- 삼성전자·SK하이닉스·TSMC·엔비디아 관련 계약 → amplifier_hit=true
- HBM/AI 서버/데이터센터 수혜 → catalyst_type="theme"
- 국산화/탈중국 정책 → catalyst_type="policy"
- "세계 1위", "독점 공급", "수주 1000억+" → amplifier_hit=true
- DRAM/NAND 가격 동향이 섹터 심리 좌우
"""

_PROMPT_POLICY = """당신은 한국 2차전지/에너지/원전 전문 주식 재료 분석가입니다.
판단 중점:
- 정부 예산/정책 발표 → catalyst_type="policy"
- IRA/탄소중립/RE100 수혜 주목
- 완성차 JV·독점공급 계약 → amplifier_hit=true
- 원전/SMR 재가동 정책 → 정책 드라이버
- 중국산 저가 공세 → decay_risk="high"
"""

_PROMPT_DEFENSE = """당신은 한국 방산/조선/기계 전문 주식 재료 분석가입니다.
판단 중점:
- K방산 수출(폴란드·사우디 등) → amplifier_hit=true
- LNG선·컨테이너선 수주잔고, 선가 → catalyst_type="contract"
- 로봇/휴머노이드 → catalyst_type="theme"
- 지정학(러·우, 중동) → 방산 모멘텀
"""

_PROMPT_FINANCE = """당신은 한국 금융/부동산 전문 주식 재료 분석가입니다.
판단 중점:
- 금리 방향 → 은행 NIM vs 증권·보험 수혜 구분
- 배당수익률 4%+ → catalyst_type="earnings"
- PF 부실·건설사 워크아웃 → bearish
- 부동산 규제 완화 → catalyst_type="policy"
"""

_PROMPT_POLITICAL = """당신은 한국 정치 테마주 전문 분석가입니다.
판단 중점:
- 실적이 아닌 기대감으로 움직임 → freshness가 가장 중요
- 뉴스 하루 지나면 decay_risk="high"
- 선거 3개월 전 freshness="first_seen", 1개월 내 → decay_risk="high"
- 구체적 정책 연결 없는 순수 "관련주" 딱지 → decay_risk="high"
- 재료 소멸이 가장 빠른 섹터
"""

_PROMPT_GENERAL = """당신은 한국 주식 재료 분석가입니다.
- 반사이익: 경쟁사 악재가 이 종목의 호재가 될 수 있는지도 고려
"""

_PROMPT_MAP = {
    "bio": _PROMPT_BIO,
    "tech": _PROMPT_TECH,
    "policy": _PROMPT_POLICY,
    "defense": _PROMPT_DEFENSE,
    "finance": _PROMPT_FINANCE,
    "political": _PROMPT_POLITICAL,
    "general": _PROMPT_GENERAL,
}


# ─────────────────────────────────────────────
# 프롬프트 선택
# ─────────────────────────────────────────────

def _select_prompt(sector: str) -> str:
    """종목 섹터 → 전문 프롬프트 타입."""
    try:
        from shared.theme_taxonomy import get_prompt_type
        return get_prompt_type(sector)
    except Exception:
        return "general"


# ─────────────────────────────────────────────
# 공개 API
# ─────────────────────────────────────────────

def analyze_material(code: str, name: str, dart: dict, news: dict, sector: str = "") -> dict:
    """Gemini 구조화 분석 → dict. v2: 섹터별 프롬프트."""
    dart_summary = "\n".join(dart.get("reasons", ["공시 정보 없음"]))
    news_items = news.get("items", [])
    news_lines = []
    for item in news_items[:5]:
        title = item.get("title", "").replace("<b>", "").replace("</b>", "")
        snippet = item.get("description", "")[:100]
        news_lines.append(f"- {title}: {snippet}")
    news_summary = "\n".join(news_lines) if news_lines else "뉴스 없음"

    # v2: 섹터별 프롬프트 선택
    prompt_type = _select_prompt(sector)
    header = _PROMPT_MAP.get(prompt_type, _PROMPT_GENERAL)

    prompt = f"""{header}

종목: {name} ({code})

DART 공시:
{dart_summary}

뉴스 제목 + 스니펫:
{news_summary}
{_OUTPUT_FORMAT}"""

    raw = _call_gemini(prompt)
    return _parse_response(raw)


def _call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        return "{}"
    time.sleep(API_SLEEP_GEMINI)
    try:
        resp = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent",
            params={"key": GEMINI_API_KEY},
            json={"contents": [{"parts": [{"text": prompt}]}]},
            timeout=API_TIMEOUT * 2,
        )
        data = resp.json()
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        return text
    except Exception as e:
        logger.warning(f"Gemini 호출 실패: {e}")
        return "{}"


def _parse_response(text: str) -> dict:
    try:
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0]
        result = json.loads(cleaned)
        result.setdefault("verdict", "neutral")
        result.setdefault("freshness", "recent")
        result.setdefault("catalyst_type", "unknown")
        result.setdefault("theme_link", "")
        result.setdefault("summary", "")
        result.setdefault("decay_risk", "mid")
        return result
    except (json.JSONDecodeError, KeyError, IndexError):
        return {
            "verdict": "neutral", "freshness": "recent",
            "catalyst_type": "unknown", "theme_link": "",
            "summary": "AI 파싱 실패", "decay_risk": "mid",
        }
