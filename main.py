"""
PADO 메인 — 통합 스케줄러 + CLI
=================================
python main.py              → 스케줄러 모드 (08:30/14:00/15:00/15:40)
python main.py --once       → 전체 1회 실행
python main.py --cb-pick    → ClosingBell 눌림목만
python main.py --scan       → 재차거시 스캔만
python main.py --wave       → 파동 스캔만
python main.py --briefing   → 아침 브리핑만
"""

import sys
import signal
import argparse
from datetime import datetime, date

from config import (
    KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL,
    setup_logging,
)
from shared.kiwoom_api import KiwoomAPI
from shared import storage
from shared.notifier import Notifier

logger = setup_logging()


class App:
    """PADO 통합 앱."""

    def __init__(self):
        logger.info("=" * 50)
        logger.info("PADO 시작")
        logger.info("=" * 50)

        # 싱글톤 초기화
        storage.init_storage()

        self.api = KiwoomAPI(
            appkey=KIWOOM_APPKEY,
            secretkey=KIWOOM_SECRETKEY,
            base_url=KIWOOM_BASE_URL,
        ) if KIWOOM_APPKEY else None

        self.notifier = Notifier()
        self._today = date.today().isoformat()

    # ─────────────────────────────────────
    # 08:30 아침 브리핑
    # ─────────────────────────────────────

    def run_morning_briefing(self):
        """🔍 재차거시 브리핑 + 🌊 파동 알림 (있으면)."""
        logger.info("── 08:30 아침 브리핑 ──")

        from jaechageosi.signal_book import SignalBook
        from jaechageosi.result_types import MarketResult
        from jaechageosi.formatter import format_morning_scan, format_wave_alert

        book = SignalBook()

        # 시황: DB에서 저장된 값 읽기 (파이프라인에서 이미 계산됨)
        market = self._load_market_from_db()

        # 어제 스캔 결과 + 감시 중
        data = book.get_morning_candidates(self._today)

        # 🔍 재차거시 브리핑
        embed = format_morning_scan(data, market)
        self.notifier.send_pado([embed])
        logger.info(f"🔍 브리핑 발송: A/B {len(data.get('scan_results', []))}건, 감시 {len(data.get('watching', []))}건")

        # 🌊 파동 알림 (있으면)
        waves = storage.get_wave_signals(self._today)
        if not waves:
            from datetime import timedelta
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            waves = storage.get_wave_signals(yesterday)
            waves = [w for w in waves if not w.get("notified")]

        if waves:
            embed_wave = format_wave_alert(waves)
            self.notifier.send_pado([embed_wave])
            logger.info(f"🌊 파동 알림: {len(waves)}건")

    def _load_market_from_db(self):
        """DB에서 가장 최근 시황 읽기. 없으면 실시간 평가."""
        import json as _json
        from jaechageosi.result_types import MarketResult
        try:
            import sqlite3
            from config import APP_DB_PATH
            conn = sqlite3.connect(str(APP_DB_PATH))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM market_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row:
                themes = _json.loads(row["leading_themes"]) if row["leading_themes"] else []
                return MarketResult(
                    date=row["date"], score=row["score"] or 0,
                    mode=row["mode"] or "mixed",
                    leading_themes=themes,
                    dangerous=bool(row["dangerous"]),
                    nasdaq_change=row["nasdaq_chg"] or 0.0,
                    kospi_ma20_gap=0.0,
                    seasonal_note="",
                    reasons=[f"DB 저장값 ({row['date']})"],
                )
        except Exception as e:
            logger.debug(f"시황 DB 읽기 실패: {e}")

        # 폴백: 실시간 평가 (캐시 없어도 키움만으로)
        from jaechageosi.market_engine import MarketEngine
        engine = MarketEngine(api=self.api)
        return engine.evaluate(self._today)

    # ─────────────────────────────────────
    # 14:00 장중 눌림목 체크
    # ─────────────────────────────────────

    def run_midday_check(self):
        """📍 감시종목 현재가 → 눌림목 진입 포착."""
        logger.info("── 14:00 장중 체크 ──")

        from closingbell.entry_watchlist import check_pullbacks
        from jaechageosi.formatter import format_midday_check

        watching = storage.get_watching()
        if not watching:
            logger.info("감시종목 0건 — 스킵")
            return

        hits = check_pullbacks(watching, api=self.api)
        if hits:
            embed = format_midday_check(hits)
            self.notifier.send_pado([embed])
            logger.info(f"📍 눌림목 포착: {len(hits)}건")
        else:
            logger.info("눌림목 해당 없음")

    # ─────────────────────────────────────
    # 15:00 ClosingBell TOP3
    # ─────────────────────────────────────

    def run_cb_pick(self):
        """🎯 형님용 ClosingBell 눌림목 TOP3."""
        logger.info("── 15:00 ClosingBell ──")

        from closingbell.screener import CBScreener
        from closingbell.entry_watchlist import check_pullbacks
        from jaechageosi.formatter import format_cb_pick

        screener = CBScreener(api=self.api)
        result = screener.run(date=self._today)

        # TOP5에서 눌림목 체크 → TOP3
        stocks = result.get("stocks", [])
        picks = check_pullbacks(stocks, api=self.api) if stocks else stocks[:3]
        if not picks:
            picks = stocks[:3]

        if picks:
            embed = format_cb_pick(picks, market_note="")
            self.notifier.send_cb([embed])
            logger.info(f"🎯 ClosingBell 발송: {len(picks)}건")

    # ─────────────────────────────────────
    # 15:40~ 스크리닝 파이프라인
    # ─────────────────────────────────────

    def run_screening_pipeline(self):
        """전체 파이프라인: OHLCV갱신 → 캐시 → 차트 → 거래량 → 재료 → 시황 → 교집합 → 파동 → 뉴스."""
        logger.info("── 15:40 파이프라인 시작 ──")

        # ① OHLCV 갱신
        try:
            from updater.fdr_update import update_ohlcv_all, update_global
            update_global()
            update_ohlcv_all()
            logger.info("① OHLCV 갱신 완료")
        except Exception as e:
            logger.error(f"① OHLCV 갱신 실패: {e}")

        # ①-b v2: OHLCV 캐시 로드 (거래대금 사이드카 동시 수집)
        from shared.ohlcv_cache import OHLCVCache
        cache = OHLCVCache.instance()
        try:
            cache.preload_all()
            logger.info(f"①-b 캐시 로드 완료: {len(cache.get_all_codes())}종목, tv_sidecar {len(cache.tv_sidecar)}건")
        except Exception as e:
            logger.error(f"①-b 캐시 로드 실패: {e}")

        # ② 글로벌 지수
        try:
            from monitor.market_context import MarketContext
            MarketContext()
            logger.info("② 글로벌 지수 완료")
        except Exception as e:
            logger.warning(f"② 글로벌 실패: {e}")

        # ③ 성과 추적
        try:
            from monitor.performance_tracker import track_today
            track_today()
            logger.info("③ 성과 추적 완료")
        except Exception as e:
            logger.warning(f"③ 성과 추적 실패: {e}")

        # ④ 차트 스캔 (전종목, 캐시 사용)
        from jaechageosi.chart_engine import ChartEngine
        chart_engine = ChartEngine()
        chart_results = chart_engine.scan_all()
        logger.info(f"④ 차트 스캔: {len(chart_results)}건 후보")

        if not chart_results:
            logger.info("차트 후보 0건 — 파이프라인 종료")
            self._run_post_pipeline(cache)
            return

        # ⑤ 거래량 (차트 통과 종목만)
        from jaechageosi.volume_engine import VolumeEngine
        vol_engine = VolumeEngine(api=self.api)
        chart_vol_pass = []
        for cr in chart_results:
            vr = vol_engine.score_single(cr.code)
            if vr and vr.score >= 20:
                chart_vol_pass.append((cr, vr))

        logger.info(f"⑤ 차트+거래량 통과: {len(chart_vol_pass)}건")

        if not chart_vol_pass:
            logger.info("차트+거래량 통과 0건 — 파이프라인 종료")
            self._run_post_pipeline(cache)
            return

        # ⑥ 시황 (1회, v2: 캐시 tv_sidecar 전달)
        from jaechageosi.market_engine import MarketEngine
        market_engine = MarketEngine(api=self.api)
        market_result = market_engine.evaluate(self._today, tv_data=cache.tv_sidecar)
        logger.info(f"⑥ 시황: {market_result.mode} (점수 {market_result.score}, 테마 {market_result.leading_themes})")

        # 시황 DB 저장
        storage.save_market_daily({
            "date": self._today, "mode": market_result.mode,
            "leading_themes": market_result.leading_themes,
            "nasdaq_chg": market_result.nasdaq_change,
            "dangerous": market_result.dangerous, "score": market_result.score,
        })

        # ⑦ 재료 (통과 종목만, v2: sector 전달)
        from jaechageosi.material_engine import MaterialEngine
        from jaechageosi.intersection import intersect
        from jaechageosi.signal_book import SignalBook
        from shared.stock_map import get_stock

        mat_engine = MaterialEngine(api=self.api)
        book = SignalBook()
        all_results = []

        for cr, vr in chart_vol_pass:
            stock = get_stock(cr.code)
            if not stock:
                continue

            mr = mat_engine.evaluate(cr.code, stock.name, sector=stock.sector)

            # ⑧ 교집합
            result = intersect(cr, vr, mr, market_result, stock)
            all_results.append(result)

            if result.grade != "REJECT":
                logger.info(
                    f"  {result.grade} {stock.name} conf={result.confidence} "
                    f"C{cr.score}/V{vr.score}/M{mr.score}/Mk{market_result.score} "
                    f"theme+{result.theme_match_bonus}"
                )

        # ⑨ DB 저장 + 감시 등록
        saved = book.upsert_scan(self._today, all_results)
        logger.info(f"⑦⑧⑨ 재차거시 완료: {saved}건 저장")

        # ⑩ 파동 스캔
        from wave.detector import WaveDetector
        detector = WaveDetector()
        waves = detector.scan_all(self._today)
        logger.info(f"⑩ 파동 스캔: {len(waves)}건")

        # ⑪ 주간 업데이트 (월요일만)
        if datetime.now().weekday() == 0:
            try:
                from updater.weekly_update import run_weekly_update
                run_weekly_update()
                logger.info("⑪ 주간 업데이트 완료")
            except Exception as e:
                logger.warning(f"⑪ 주간 실패: {e}")

        # ⑫ v2: 뉴스 수집
        try:
            from checkers.news_collector import collect_daily_news
            count = collect_daily_news(self._today)
            logger.info(f"⑫ 뉴스 수집: {count}건")
        except Exception as e:
            logger.warning(f"⑫ 뉴스 수집 실패: {e}")

        logger.info("── 파이프라인 종료 ──")

    def _run_post_pipeline(self, cache=None):
        """파이프라인 조기 종료 시에도 실행해야 할 작업."""
        # ⑩ 파동 스캔
        try:
            from wave.detector import WaveDetector
            detector = WaveDetector()
            waves = detector.scan_all(self._today)
            logger.info(f"⑩ 파동 스캔: {len(waves)}건")
        except Exception as e:
            logger.warning(f"⑩ 파동 실패: {e}")

        # ⑫ 뉴스 수집 (매일 필수)
        try:
            from checkers.news_collector import collect_daily_news
            count = collect_daily_news(self._today)
            logger.info(f"⑫ 뉴스 수집: {count}건")
        except Exception as e:
            logger.warning(f"⑫ 뉴스 수집 실패: {e}")

        logger.info("── 파이프라인 종료 (조기) ──")

    # ─────────────────────────────────────
    # 전체 1회 실행
    # ─────────────────────────────────────

    def _is_trading_day(self) -> bool:
        """오늘이 거래일인지 판단 (주말 + KRX 공휴일)."""
        from datetime import date
        today = date.today()
        # 주말 체크
        if today.weekday() >= 5:  # 토(5), 일(6)
            return False
        # KRX 공휴일 체크
        try:
            import json
            from config import KRX_HOLIDAYS
            if KRX_HOLIDAYS.exists():
                with open(KRX_HOLIDAYS, "r", encoding="utf-8") as f:
                    holidays = json.load(f)
                if today.isoformat() in holidays:
                    return False
        except Exception:
            pass
        return True

    def run_morning(self):
        """아침 자동 실행 (08:25 트리거용).
        거래일: 브리핑 웹훅 (전일 결과)
        주말/공휴일: 뉴스 수집 + 글로벌 갱신
        """
        if self._is_trading_day():
            logger.info("=== 거래일 아침 — 브리핑 ===")
            self.run_morning_briefing()
        else:
            logger.info("=== 비거래일 — 뉴스 수집 ===")
            self.run_weekend()

    def run_once(self):
        """전체 파이프라인 1회 실행 (거래일 자동 판단)."""
        if self._is_trading_day():
            logger.info("=== 거래일 — 전체 파이프라인 ===")
            self.run_screening_pipeline()
            self.run_morning_briefing()
        else:
            logger.info("=== 비거래일 — 뉴스 수집만 ===")
            self.run_weekend()

    def run_test_all(self):
        """전체 스케줄 시뮬레이션 (주말에도 강제 실행, 웹훅 전부 발송)."""
        logger.info("=" * 50)
        logger.info("=== 전체 스케줄 테스트 (08:30→14:00→15:00→15:40) ===")
        logger.info("=" * 50)

        # 15:40 파이프라인 (먼저 — 데이터가 있어야 브리핑 가능)
        logger.info("\n── [15:40] 파이프라인 ──")
        self.run_screening_pipeline()

        # 08:30 아침 브리핑 (전일 결과 + 파동)
        logger.info("\n── [08:30] 아침 브리핑 ──")
        self.run_morning_briefing()

        # 15:00 ClosingBell TOP3
        logger.info("\n── [15:00] ClosingBell TOP3 ──")
        self.run_cb_pick()

        # 14:00 장중 눌림목 (감시종목 있을 때만)
        logger.info("\n── [14:00] 장중 눌림목 체크 ──")
        self.run_midday_check()

        logger.info("=" * 50)
        logger.info("=== 전체 스케줄 테스트 완료 ===")
        logger.info("디스코드에서 웹훅 4종 확인:")
        logger.info("  🔍 재차거시 브리핑")
        logger.info("  🌊 파동 알림")
        logger.info("  🎯 ClosingBell TOP3")
        logger.info("  📍 눌림목 포착 (감시종목 있을 때)")
        logger.info("=" * 50)

    def run_weekend(self):
        """비거래일: 뉴스 수집 + 글로벌 지수 업데이트만."""
        # 글로벌 지수 (나스닥 금요일 종가 갱신)
        try:
            from updater.fdr_update import update_global
            update_global()
            logger.info("글로벌 지수 갱신 완료")
        except Exception as e:
            logger.warning(f"글로벌 갱신 실패: {e}")

        # 뉴스 수집 (매일 축적)
        try:
            from checkers.news_collector import collect_daily_news
            count = collect_daily_news(self._today)
            logger.info(f"뉴스 수집: {count}건")
        except Exception as e:
            logger.warning(f"뉴스 수집 실패: {e}")

        logger.info("=== 비거래일 종료 ===")

    # ─────────────────────────────────────
    # 하루 자동 운영 (BAT 1개로 전부)
    # ─────────────────────────────────────

    def run_daily(self):
        """하루 전체 자동 운영. 08:25 기동 → 시간별 작업 → 완료 후 종료.

        거래일:
          08:25 기동 → 즉시 아침 브리핑
          14:00 대기 → 장중 눌림목
          15:00 대기 → ClosingBell TOP3
          15:35 대기 → 전체 파이프라인
          ~16:00 → 자동 종료

        비거래일:
          08:25 기동 → 뉴스 수집 + 글로벌 → 즉시 종료
        """
        import time as _time
        from datetime import datetime

        if not self._is_trading_day():
            logger.info("=" * 50)
            logger.info("=== 비거래일 — 뉴스 수집 후 종료 ===")
            logger.info("=" * 50)
            self.run_weekend()
            return

        logger.info("=" * 50)
        logger.info("=== 거래일 하루 운영 시작 ===")
        logger.info("=" * 50)

        # 작업 스케줄 (시:분, 함수, 설명)
        tasks = [
            ("08:25", self.run_morning_briefing, "🔍 아침 브리핑"),
            ("14:00", self.run_midday_check,     "📍 장중 눌림목"),
            ("15:00", self.run_cb_pick,           "🎯 ClosingBell TOP3"),
            ("15:35", self.run_screening_pipeline,"📊 전체 파이프라인"),
        ]

        now = datetime.now()
        executed = set()

        for sched_time, func, label in tasks:
            h, m = map(int, sched_time.split(":"))
            target = now.replace(hour=h, minute=m, second=0, microsecond=0)

            # 이미 지난 시간이면 즉시 실행 (늦게 켜진 경우)
            if datetime.now() > target:
                if sched_time not in executed:
                    logger.info(f"\n── [{sched_time}] {label} (지난 시간 — 즉시 실행) ──")
                    try:
                        func()
                    except Exception as e:
                        logger.error(f"{label} 실패: {e}")
                    executed.add(sched_time)
                continue

            # 대기
            wait = (target - datetime.now()).total_seconds()
            if wait > 0:
                logger.info(f"⏳ [{sched_time}] {label} 대기 중... ({wait/60:.0f}분 남음)")
                while datetime.now() < target:
                    _time.sleep(30)
                    # 1분마다 heartbeat
                    remaining = (target - datetime.now()).total_seconds()
                    if int(remaining) % 300 < 30:  # 5분마다 로그
                        logger.info(f"    ⏳ {remaining/60:.0f}분 남음")

            logger.info(f"\n── [{sched_time}] {label} ──")
            try:
                func()
            except Exception as e:
                logger.error(f"{label} 실패: {e}")
            executed.add(sched_time)

        logger.info("=" * 50)
        logger.info("=== 하루 운영 완료 — 종료 ===")
        logger.info(f"=== 실행된 작업: {len(executed)}개 ===")
        logger.info("=" * 50)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PADO — 파동매매 + 재차거시 시스템")
    parser.add_argument("--once", action="store_true", help="전체 1회 실행")
    parser.add_argument("--morning", action="store_true", help="아침 (거래일=브리핑, 주말=뉴스)")
    parser.add_argument("--cb-pick", action="store_true", help="ClosingBell TOP3만")
    parser.add_argument("--scan", action="store_true", help="재차거시 스캔만")
    parser.add_argument("--wave", action="store_true", help="파동 스캔만")
    parser.add_argument("--briefing", action="store_true", help="아침 브리핑만")
    parser.add_argument("--midday", action="store_true", help="장중 체크만")
    parser.add_argument("--weekend", action="store_true", help="비거래일 모드 (뉴스+글로벌만)")
    parser.add_argument("--test-all", action="store_true", help="전체 스케줄 테스트 (웹훅 4종 강제 발송)")

    args = parser.parse_args()
    app = App()

    if args.test_all:
        app.run_test_all()
    elif args.morning:
        app.run_morning()
    elif args.once:
        app.run_once()
    elif args.cb_pick:
        app.run_cb_pick()
    elif args.scan:
        app.run_screening_pipeline()
    elif args.weekend:
        app.run_weekend()
    elif args.wave:
        from wave.detector import WaveDetector
        detector = WaveDetector()
        signals = detector.scan_all(date.today().isoformat())
        for s in signals:
            print(f"  {s.wave_type} {s.name} 강도={s.strength} {s.reasons}")
    elif args.briefing:
        app.run_morning_briefing()
    elif args.midday:
        app.run_midday_check()
    else:
        app.run_daily()


if __name__ == "__main__":
    main()
