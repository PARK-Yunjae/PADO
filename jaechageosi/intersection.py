"""
교집합 로직 — 4채널 AND + 등급 + 시너지
=========================================
순수 계산 모듈. 외부 의존성 없음 (DataFrame 불필요).
"""

from config import JCGS_PASS, JCGS_FAIL, JCGS_WEIGHT
from shared.schemas import StockBasic
from jaechageosi.result_types import (
    ChartResult, VolumeResult, MaterialResult, MarketResult,
    IntersectionResult, Grade,
)

_GRADE_ORDER = {"A": 0, "B": 1, "C": 2, "REJECT": 3}
_ACTIONS = {"A": "pullback_entry", "B": "watch", "C": "record_only", "REJECT": "reject"}


def _min_grade(grade: Grade, cap: Grade) -> Grade:
    if _GRADE_ORDER.get(grade, 3) < _GRADE_ORDER.get(cap, 3):
        return cap
    return grade


def _judge(channel: str, score: int) -> str:
    """pass / gray / fail."""
    if score >= JCGS_PASS[channel]:
        return "pass"
    if score >= JCGS_FAIL[channel]:
        return "gray"
    return "fail"


def intersect(
    chart: ChartResult,
    volume: VolumeResult,
    material: MaterialResult,
    market: MarketResult,
    stock: StockBasic,
) -> IntersectionResult:
    """4채널 교집합 → A/B/C/REJECT + confidence."""

    # ── 0. 절대 거부권 ──
    if material.dart_grade <= 1:
        return _reject(chart, volume, material, market, stock, "DART 극위험")
    if material.score < JCGS_FAIL["material"]:
        return _reject(chart, volume, material, market, stock, "재료 fail")
    if market.dangerous:
        return _reject(chart, volume, material, market, stock, "시황 risk_off")

    # ── 1. pass/gray/fail 판정 ──
    verdicts = {
        "chart":    _judge("chart", chart.score),
        "volume":   _judge("volume", volume.score),
        "material": _judge("material", material.score),
        "market":   _judge("market", market.score),
    }

    fail_count = sum(1 for v in verdicts.values() if v == "fail")
    pass_count = sum(1 for v in verdicts.values() if v == "pass")

    if fail_count > 0:
        failed = [k for k, v in verdicts.items() if v == "fail"]
        return _reject(chart, volume, material, market, stock, f"{','.join(failed)} fail")

    # ── 2. 등급 산정 ──
    if pass_count == 4:
        grade: Grade = "A"
    elif pass_count == 3:
        grade = "B"
    elif pass_count == 2:
        grade = "C"
    else:
        return _reject(chart, volume, material, market, stock, "pass 부족")

    # ── 3. 실행 보정 ──
    if chart.chart_state == "extended":
        grade = _min_grade(grade, "B")
    if volume.flow_state == "chasing":
        grade = _min_grade(grade, "B")
    if market.mode == "risk_off":
        grade = _min_grade(grade, "C")

    # ── 4. 보너스 ──
    theme_bonus = _calc_theme_match(material, market, stock)
    synergy = _calc_synergy(chart, volume, material)

    # ── 5. confidence ──
    confidence = round(
        chart.score * JCGS_WEIGHT["chart"]
        + volume.score * JCGS_WEIGHT["volume"]
        + material.score * JCGS_WEIGHT["material"]
        + market.score * JCGS_WEIGHT["market"]
        + theme_bonus + synergy
    )
    confidence = max(0, min(confidence, 100))

    return IntersectionResult(
        code=stock.code, name=stock.name,
        grade=grade, confidence=confidence,
        action=_ACTIONS[grade], reject_reason=None,
        chart=chart, volume=volume, material=material, market=market,
        theme_match_bonus=theme_bonus, synergy_bonus=synergy,
    )


def _reject(chart, volume, material, market, stock, reason) -> IntersectionResult:
    return IntersectionResult(
        code=stock.code, name=stock.name,
        grade="REJECT", confidence=0,
        action="reject", reject_reason=reason,
        chart=chart, volume=volume, material=material, market=market,
    )


def _calc_theme_match(material: MaterialResult, market: MarketResult, stock: StockBasic) -> int:
    """종목-시황 테마 매칭 보너스 (v2: mega 레벨 매칭)."""
    from shared.theme_taxonomy import theme_match_score
    return theme_match_score(
        material_theme=material.theme_link,
        market_themes=market.leading_themes,
        stock_sector=stock.sector,
        stock_themes=stock.themes,
    )


def _calc_synergy(chart: ChartResult, volume: VolumeResult, material: MaterialResult) -> int:
    """채널간 시너지 보너스."""
    bonus = 0
    # 거감음봉 + OBV 다이버전스 동시
    if volume.gge_strict and volume.obv_bull_div:
        bonus += 8
    # 파동 + 악재 소멸 (유목민 완전체)
    if "wave1" in chart.signal_family and material.freshness == "first_seen":
        bonus += 10
    elif "wave2" in chart.signal_family and material.catalyst_type == "relief":
        bonus += 8
    return min(bonus, 15)
