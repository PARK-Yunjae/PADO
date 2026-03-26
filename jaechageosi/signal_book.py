"""감시명부 — 스캔 결과 저장 + 감시 종목 관리."""

from shared import storage
from jaechageosi.result_types import IntersectionResult
from config import WATCHLIST_MAX_HOLD_DAYS, WATCHLIST_MAX_ITEMS, setup_logging

logger = setup_logging().getChild("signal_book")


class SignalBook:

    def upsert_scan(self, date: str, results: list[IntersectionResult]) -> int:
        """스캔 결과 저장 + A/B 자동 감시 등록."""
        saved = 0
        for r in results:
            row = {
                "scan_date": date, "code": r.code, "name": r.name,
                "grade": r.grade, "confidence": r.confidence, "action": r.action,
                "chart_state": r.chart.chart_state, "flow_state": r.volume.flow_state,
                "chart_score": r.chart.score, "volume_score": r.volume.score,
                "material_score": r.material.score, "market_score": r.market.score,
                "theme_match": r.theme_match_bonus, "synergy": r.synergy_bonus,
                "reject_reason": r.reject_reason,
                # v4.1
                "signal_type": r.signal_type,
                "recommended_hold_days": r.recommended_hold_days,
                "strategy_bucket": r.strategy_bucket,
            }
            sid = storage.save_scan_result(row)
            if sid:
                saved += 1

            # A/B → 감시 등록
            if r.grade in ("A", "B"):
                watching = storage.get_watching()
                if len(watching) < WATCHLIST_MAX_ITEMS:
                    storage.add_watchlist({
                        "code": r.code, "name": r.name, "grade": r.grade,
                        "added_date": date, "source": "scan",
                        "scan_result_id": sid,
                        "entry_price": r.chart.entry_price,
                        "stop_loss": r.chart.stop_loss,
                        "target_price": r.chart.target_price,
                    })
                    logger.info(f"감시 등록: {r.name} ({r.grade})")

        # 만료 처리
        expired = storage.expire_old_watchlist(WATCHLIST_MAX_HOLD_DAYS)
        if expired:
            logger.info(f"감시 만료: {expired}건")

        return saved

    def get_watching(self) -> list[dict]:
        return storage.get_watching()

    def get_morning_candidates(self, date: str) -> dict:
        """08:30 브리핑용 — 어제 A/B + 진행중 감시."""
        from datetime import datetime, timedelta

        scan = storage.get_scan_results(date)
        watching = storage.get_watching()

        # 오늘 결과 없으면 최근 거래일 fallback
        if not scan:
            d = datetime.strptime(date, "%Y-%m-%d").date()
            for i in range(1, 5):
                prev = (d - timedelta(days=i)).isoformat()
                scan = storage.get_scan_results(prev)
                if scan:
                    break

        ab_scan = [r for r in scan if r.get("grade") in ("A", "B")]
        return {"scan_results": ab_scan, "watching": watching}
