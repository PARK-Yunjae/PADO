"""수급 API — 기관/외인 매매 + 공매도."""

import time
from config import API_SLEEP_KIWOOM, setup_logging

logger = setup_logging().getChild("supply_api")


def check_supply(code: str, api) -> dict:
    """키움 API로 수급 데이터 조회 → {score, inst_foreign_5d, short_ratio, reasons}."""
    score = 0
    reasons = []
    inst_foreign_5d = False
    short_ratio = 0.0

    if not api:
        return {"score": 0, "inst_foreign_5d": False, "short_ratio": 0.0,
                "reasons": ["API 미설정"]}

    # ── 기관/외인 매매 (ka10045 기관매매추이) ──
    try:
        time.sleep(API_SLEEP_KIWOOM)
        # ka10045 또는 get_broker_ranking으로 기관/외인 순매수 확인
        broker_data = api.get_broker_ranking(code, period="5")
        if broker_data:
            buy_total = sum(b.get("buy_amount", 0) for b in broker_data[:5])
            sell_total = sum(b.get("sell_amount", 0) for b in broker_data[:5])
            if buy_total > sell_total * 1.2:
                score += 15
                inst_foreign_5d = True
                reasons.append("기관외인 5일 순매수 +15")
            elif buy_total > sell_total:
                score += 8
                reasons.append("외인 단독 순매수 +8")
            elif sell_total > buy_total * 1.5:
                score -= 10
                reasons.append("기관외인 동반 순매도 -10")
    except Exception as e:
        logger.debug(f"기관매매 조회 실패 {code}: {e}")
        reasons.append("기관매매 조회 실패")

    # ── 공매도 (ka10014) ──
    try:
        time.sleep(API_SLEEP_KIWOOM)
        shorts = api.get_short_selling(code, days=5)
        if shorts:
            avg_ratio = sum(s.get("short_ratio", 0) for s in shorts) / len(shorts)
            short_ratio = round(avg_ratio, 2)
            if short_ratio < 3:
                score += 7
                reasons.append(f"공매도 {short_ratio}% (낮음) +7")
            elif short_ratio > 5:
                score -= 5
                reasons.append(f"공매도 {short_ratio}% (높음) -5")
            else:
                reasons.append(f"공매도 {short_ratio}% (보통)")
    except Exception as e:
        logger.debug(f"공매도 조회 실패 {code}: {e}")
        reasons.append("공매도 조회 실패")

    return {
        "score": max(-10, min(score, 35)),
        "inst_foreign_5d": inst_foreign_5d,
        "short_ratio": short_ratio,
        "reasons": reasons,
    }
