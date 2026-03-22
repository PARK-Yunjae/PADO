"""DART 공시 6단계 분류."""

import time
import json
import requests
from config import DART_API_KEY, DART_CORP_MAP, API_SLEEP_DART, API_TIMEOUT, setup_logging

logger = setup_logging().getChild("dart_checker")

# 6단계 공시 키워드
_DANGER_KEYWORDS = ["상장폐지", "감사의견거절", "감사의견한정"]
_RISK_KEYWORDS = ["유상증자", "감자", "횡령", "배임", "자사주매각"]
_WARN_KEYWORDS = ["전환사채", "신주인수권", "CB", "BW"]
_GOOD_KEYWORDS = ["자사주매입", "자사주취득", "투자유치", "수주"]
_GREAT_KEYWORDS = ["무상증자", "세계최초", "국내최초", "대규모수주"]


def check_dart(code: str) -> dict:
    """DART 공시 6단계 → {grade: 1~6, score: 0~70, reasons: [...]}."""
    corp_code = _get_corp_code(code)
    if not corp_code:
        return {"grade": 4, "score": 40, "reasons": ["DART 매핑 없음"]}

    disclosures = _get_disclosures(corp_code, days=30)
    if not disclosures:
        return {"grade": 4, "score": 40, "reasons": ["최근 30일 공시 없음 (중립)"]}

    titles = " ".join(d.get("report_nm", "") for d in disclosures)
    reasons = []

    # 1단계: 극위험 (0점)
    for kw in _DANGER_KEYWORDS:
        if kw in titles:
            return {"grade": 1, "score": 0, "reasons": [f"극위험: {kw}"]}

    # 2단계: 위험 (10점)
    for kw in _RISK_KEYWORDS:
        if kw in titles:
            reasons.append(f"위험: {kw}")
            return {"grade": 2, "score": 10, "reasons": reasons}

    # 3단계: 주의 (25점)
    for kw in _WARN_KEYWORDS:
        if kw in titles:
            reasons.append(f"주의: {kw}")
            return {"grade": 3, "score": 25, "reasons": reasons}

    # 5단계: 호재 (55점)
    for kw in _GOOD_KEYWORDS:
        if kw in titles:
            reasons.append(f"호재: {kw}")
            return {"grade": 5, "score": 55, "reasons": reasons}

    # 6단계: 강호재 (70점)
    for kw in _GREAT_KEYWORDS:
        if kw in titles:
            reasons.append(f"강호재: {kw}")
            return {"grade": 6, "score": 70, "reasons": reasons}

    return {"grade": 4, "score": 40, "reasons": ["공시 중립"]}


def _get_corp_code(stock_code: str) -> str:
    try:
        if DART_CORP_MAP.exists():
            with open(DART_CORP_MAP, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            return mapping.get(stock_code, "")
    except Exception:
        pass
    return ""


def _get_disclosures(corp_code: str, days: int = 30) -> list:
    if not DART_API_KEY:
        return []
    try:
        from datetime import datetime, timedelta
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        time.sleep(API_SLEEP_DART)
        resp = requests.get(
            "https://opendart.fss.or.kr/api/list.json",
            params={"crtfc_key": DART_API_KEY, "corp_code": corp_code,
                    "bgn_de": start, "end_de": end, "page_count": 20},
            timeout=API_TIMEOUT,
        )
        data = resp.json()
        return data.get("list", []) if data.get("status") == "000" else []
    except Exception as e:
        logger.warning(f"DART API 실패: {e}")
        return []
