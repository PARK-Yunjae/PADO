"""
PADO v2 주말 통합 테스트
=========================
주말에 실행해서 모든 모듈이 정상 동작하는지 확인.
실제 API 호출 + 실제 웹훅 발송 포함.

사용법:
  python tools/weekend_test.py              # 전체 (웹훅 포함)
  python tools/weekend_test.py --no-webhook # 웹훅 제외
  python tools/weekend_test.py --step 3     # 특정 단계만
"""

import sys
import os
import time
import argparse
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (
    OHLCV_DIR, KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL,
    NAVER_CLIENT_ID, GEMINI_API_KEY, DISCORD_WEBHOOK,
    JCGS_PASS, setup_logging,
)
from shared import storage

logger = setup_logging()
TODAY = date.today().isoformat()
# 가장 최근 거래일 (금요일)
LAST_TRADING_DAY = (date.today() - timedelta(days=max(1, date.today().weekday() - 4))).isoformat()

P = "✅"
F = "❌"
W = "⚠️"
I = "📋"


def test1_cache_and_sidecar():
    """1. OHLCV 캐시 + 거래대금 변화율 (소스2 핵심)"""
    print("\n" + "="*60)
    print("TEST 1: OHLCV 캐시 + 거래대금 변화율 사이드카")
    print("="*60)

    from shared.ohlcv_cache import OHLCVCache
    OHLCVCache.reset()
    cache = OHLCVCache.instance()

    t0 = time.time()
    cache.preload_all()
    elapsed = time.time() - t0

    loaded = len(cache.get_all_codes())
    tv = len(cache.tv_sidecar)
    print(f"  {P} 캐시 로드: {loaded}종목, {elapsed:.1f}초, tv_sidecar {tv}건")

    # 거래대금 변화율 필터
    from shared.theme_taxonomy import normalize_sector
    from collections import Counter

    hot = [(code, d) for code, d in cache.tv_sidecar.items()
           if d["tv_today"] >= 3e9 and d["change_pct"] >= 100]
    print(f"  {I} 거래대금 변화율 통과: {len(hot)}종목")

    # 섹터 → canon 매핑 테스트
    canon_counts = Counter()
    mapped = 0
    for code, d in hot:
        canon = normalize_sector(d["sector"])
        if canon:
            canon_counts[canon] += 1
            mapped += 1

    pct = mapped / len(hot) * 100 if hot else 0
    print(f"  {P if pct > 70 else F} 섹터 매핑률: {pct:.0f}% ({mapped}/{len(hot)})")
    print(f"  {I} TOP 섹터: {canon_counts.most_common(5)}")

    return cache


def test2_market_engine(cache):
    """2. 시황 엔진 3소스 병합"""
    print("\n" + "="*60)
    print("TEST 2: 시황 엔진 — 3소스 병합")
    print("="*60)

    api = None
    if KIWOOM_APPKEY:
        from shared.kiwoom_api import KiwoomAPI
        try:
            api = KiwoomAPI(appkey=KIWOOM_APPKEY, secretkey=KIWOOM_SECRETKEY, base_url=KIWOOM_BASE_URL)
            print(f"  {P} 키움 API 연결")
        except Exception as e:
            print(f"  {W} 키움 API 실패: {e}")

    from jaechageosi.market_engine import MarketEngine
    engine = MarketEngine(api=api)
    result = engine.evaluate(LAST_TRADING_DAY, tv_data=cache.tv_sidecar)

    print(f"  {I} 점수: {result.score}, 모드: {result.mode}")
    print(f"  {P if result.leading_themes else F} 주도테마: {result.leading_themes}")
    print(f"  {I} 나스닥: {result.nasdaq_change:+.2f}%")
    for r in result.reasons:
        print(f"    - {r}")

    # 핵심 체크: 테마가 빈 문자열이 아닌지
    non_empty = [t for t in result.leading_themes if t]
    print(f"\n  {P if non_empty else F} 비공백 테마 {len(non_empty)}개")

    return result, api


def test3_chart_volume(cache):
    """3. 차트 + 거래량 파이프라인"""
    print("\n" + "="*60)
    print("TEST 3: 차트 스캔 + 거래량 필터")
    print("="*60)

    from jaechageosi.chart_engine import ChartEngine
    from jaechageosi.volume_engine import VolumeEngine

    chart_engine = ChartEngine()
    t0 = time.time()
    chart_results = chart_engine.scan_all()
    chart_time = time.time() - t0

    print(f"  {P} 차트 스캔: {len(chart_results)}건, {chart_time:.1f}초")
    if chart_results:
        print(f"    TOP5: {', '.join(f'{r.code}({r.score})' for r in chart_results[:5])}")

    vol_engine = VolumeEngine()
    passed = []
    for cr in chart_results:
        vr = vol_engine.score_single(cr.code)
        if vr:
            passed.append((cr, vr))

    pass25 = [(cr, vr) for cr, vr in passed if vr.score >= JCGS_PASS["volume"]]
    pass20 = [(cr, vr) for cr, vr in passed if vr.score >= 20]

    print(f"  {I} 거래량 점수 분포:")
    for cr, vr in passed[:10]:
        from shared.stock_map import get_stock
        name = get_stock(cr.code).name if get_stock(cr.code) else cr.code
        status = "✓pass" if vr.score >= JCGS_PASS["volume"] else "gray"
        print(f"    {name:15s} V={vr.score:3d} ({status}) {vr.flow_state}")

    print(f"  {I} pass≥25: {len(pass25)}건, pass≥20: {len(pass20)}건")

    return pass25 or pass20[:10]


def test4_material(candidates, api):
    """4. 재료 채널 (DART + 뉴스 + Gemini)"""
    print("\n" + "="*60)
    print("TEST 4: 재료 채널 (1종목만)")
    print("="*60)

    if not candidates:
        print(f"  {W} 후보 0건 — 스킵")
        return None

    from jaechageosi.material_engine import MaterialEngine
    from shared.stock_map import get_stock

    mat_engine = MaterialEngine(api=api)
    cr, vr = candidates[0]
    stock = get_stock(cr.code)
    if not stock:
        print(f"  {F} 종목 매핑 실패")
        return None

    print(f"  {I} 테스트 종목: {stock.name} ({stock.code}) 섹터: {stock.sector}")

    mr = mat_engine.evaluate(cr.code, stock.name, sector=stock.sector)

    print(f"  {I} 재료 점수: {mr.score}")
    print(f"  {I} DART: {mr.dart_grade}등급")
    print(f"  {I} 촉매: {mr.catalyst_type}")
    print(f"  {I} 신선도: {mr.freshness}")
    print(f"  {I} theme_link: {mr.theme_link}")
    print(f"  {I} 요약: {mr.headline_summary}")

    # theme_link가 canon인지 확인
    from shared.theme_taxonomy import resolve_keyword
    canon = resolve_keyword(mr.theme_link)
    print(f"  {P if canon else W} theme_link → canon: {canon or '미매핑'}")

    return mr


def test5_intersection(candidates, market_result):
    """5. 교집합 (theme_match 보너스 확인)"""
    print("\n" + "="*60)
    print("TEST 5: 교집합 — 테마 매칭 보너스")
    print("="*60)

    if not candidates:
        print(f"  {W} 후보 0건 — 스킵")
        return

    from jaechageosi.material_engine import MaterialEngine
    from jaechageosi.intersection import intersect
    from shared.stock_map import get_stock

    mat_engine = MaterialEngine()
    results = []

    for cr, vr in candidates[:6]:
        stock = get_stock(cr.code)
        if not stock:
            continue
        mr = mat_engine.evaluate(cr.code, stock.name, sector=stock.sector)
        result = intersect(cr, vr, mr, market_result, stock)
        results.append(result)

        icon = "🟢" if result.grade == "A" else "🟡" if result.grade in ("B","C") else "⚫"
        print(f"  {icon} {result.grade} {stock.name:15s} "
              f"conf={result.confidence} C{cr.score}/V{vr.score}/M{mr.score}/Mk{market_result.score} "
              f"theme+{result.theme_match_bonus} syn+{result.synergy_bonus}")

    a_count = sum(1 for r in results if r.grade == "A")
    b_count = sum(1 for r in results if r.grade == "B")
    theme_hit = sum(1 for r in results if r.theme_match_bonus > 0)
    print(f"\n  {I} A등급: {a_count}, B등급: {b_count}, 테마매칭: {theme_hit}건")

    return results


def test6_webhook(results=None, market_result=None):
    """6. 디스코드 웹훅 실발송"""
    print("\n" + "="*60)
    print("TEST 6: 디스코드 웹훅 실발송")
    print("="*60)

    if not DISCORD_WEBHOOK:
        print(f"  {F} DISCORD_WEBHOOK 미설정")
        print(f"    .env에 추가: DISCORD_WEBHOOK=https://discord.com/api/webhooks/...")
        return False

    from shared.notifier import Notifier, embed, field, COLOR_GREEN, COLOR_BLUE

    notifier = Notifier()

    # 테스트 1: 간단한 테스트 메시지
    test_embed = embed(
        title="🧪 PADO v2 주말 테스트",
        description=f"통합 테스트 실행 ({TODAY})",
        color=COLOR_GREEN,
        fields=[
            field("시스템", "정상 작동", inline=True),
            field("캐시", "2881종목", inline=True),
            field("PASS_VOL", str(JCGS_PASS["volume"]), inline=True),
        ],
        footer="주말 자동 테스트",
    )
    ok1 = notifier.send_pado([test_embed])
    print(f"  {P if ok1 else F} PADO 채널 발송: {'성공' if ok1 else '실패'}")

    ok2 = notifier.send_cb([test_embed])
    print(f"  {P if ok2 else F} CB 채널 발송: {'성공' if ok2 else '실패'}")

    # 테스트 2: 실제 브리핑 포맷
    if results and market_result:
        from jaechageosi.formatter import format_morning_scan
        data = {
            "scan_results": [
                {
                    "grade": r.grade, "name": r.name, "code": r.code,
                    "confidence": r.confidence,
                    "chart_state": r.chart.chart_state,
                    "flow_state": r.volume.flow_state,
                    "chart_score": r.chart.score,
                    "volume_score": r.volume.score,
                    "material_score": r.material.score,
                    "market_score": r.market.score,
                    "theme_match": r.theme_match_bonus,
                    "synergy": r.synergy_bonus,
                }
                for r in results if r.grade != "REJECT"
            ],
            "watching": [],
        }
        briefing_embed = format_morning_scan(data, market_result)
        ok3 = notifier.send_pado([briefing_embed])
        print(f"  {P if ok3 else F} 브리핑 포맷 발송: {'성공' if ok3 else '실패'}")

    return ok1


def test7_news_collection():
    """7. 뉴스 수집 (주말에도 실행)"""
    print("\n" + "="*60)
    print("TEST 7: 뉴스 수집")
    print("="*60)

    from checkers.news_collector import collect_daily_news
    count = collect_daily_news(TODAY)
    print(f"  {P if count > 0 else F} 수집: {count}건")

    # DB 확인
    rows = storage.get_today_news(TODAY)
    print(f"  {I} DB 저장 확인: {len(rows)}건")
    if rows:
        for r in rows[:3]:
            print(f"    [{r.get('source','')}] {r['title'][:50]}")

    return count > 0


def test8_db_integrity():
    """8. DB 무결성 + 데이터 확인"""
    print("\n" + "="*60)
    print("TEST 8: DB 무결성")
    print("="*60)

    import sqlite3
    from config import APP_DB_PATH

    conn = sqlite3.connect(str(APP_DB_PATH))
    conn.row_factory = sqlite3.Row

    tables = {
        "cb_screen_runs": 0,
        "wave_signals": 0,
        "jcgs_scan_results": 0,
        "jcgs_watchlist": 0,
        "market_daily": 0,
        "news_daily": 0,
        "performance": 0,
        "notifications": 0,
    }

    for t in tables:
        try:
            row = conn.execute(f"SELECT COUNT(*) as cnt FROM {t}").fetchone()
            tables[t] = row["cnt"]
        except Exception:
            tables[t] = -1

    for t, cnt in tables.items():
        status = P if cnt >= 0 else F
        print(f"  {status} {t:25s}: {cnt}건")

    # 최근 market_daily 확인
    rows = conn.execute("SELECT date, mode, leading_themes, score FROM market_daily ORDER BY date DESC LIMIT 3").fetchall()
    if rows:
        print(f"\n  {I} 최근 시황:")
        for r in rows:
            print(f"    {r['date']} {r['mode']:15s} themes={r['leading_themes']} score={r['score']}")

    conn.close()
    return True


# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PADO v2 주말 통합 테스트")
    parser.add_argument("--no-webhook", action="store_true", help="웹훅 발송 제외")
    parser.add_argument("--step", type=str, default="", help="특정 단계만 (예: 1,2,5)")
    args = parser.parse_args()

    storage.init_storage()

    if args.step:
        steps = [int(s) for s in args.step.split(",")]
    else:
        steps = [1, 2, 3, 4, 5, 6, 7, 8]

    if args.no_webhook and 6 in steps:
        steps.remove(6)

    print("="*60)
    print(f"  PADO v2 주말 통합 테스트 — {TODAY}")
    print(f"  실행 단계: {steps}")
    print("="*60)

    cache = None
    market_result = None
    api = None
    candidates = None
    results = None

    for s in steps:
        try:
            if s == 1:
                cache = test1_cache_and_sidecar()
            elif s == 2:
                if cache is None:
                    cache = test1_cache_and_sidecar()
                market_result, api = test2_market_engine(cache)
            elif s == 3:
                if cache is None:
                    cache = test1_cache_and_sidecar()
                candidates = test3_chart_volume(cache)
            elif s == 4:
                if candidates is None:
                    if cache is None: cache = test1_cache_and_sidecar()
                    candidates = test3_chart_volume(cache)
                if api is None and KIWOOM_APPKEY:
                    from shared.kiwoom_api import KiwoomAPI
                    api = KiwoomAPI(appkey=KIWOOM_APPKEY, secretkey=KIWOOM_SECRETKEY, base_url=KIWOOM_BASE_URL)
                test4_material(candidates, api)
            elif s == 5:
                if candidates is None or market_result is None:
                    if cache is None: cache = test1_cache_and_sidecar()
                    if market_result is None: market_result, api = test2_market_engine(cache)
                    if candidates is None: candidates = test3_chart_volume(cache)
                results = test5_intersection(candidates, market_result)
            elif s == 6:
                test6_webhook(results, market_result)
            elif s == 7:
                test7_news_collection()
            elif s == 8:
                test8_db_integrity()
        except Exception as e:
            print(f"\n  {F} Step {s} 예외: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*60)
    print("  주말 통합 테스트 완료")
    print("="*60)


if __name__ == "__main__":
    main()
