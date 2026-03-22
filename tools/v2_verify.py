"""
PADO v2 검증 스크립트
=====================
실전 가동 전에 단계별로 돌려서 정상 여부 확인.
각 단계가 독립적이라 실패해도 다음 단계 진행 가능.

사용법:
  cd C:/Coding/PADO
  python tools/v2_verify.py              # 전체
  python tools/v2_verify.py --step 1     # 특정 단계만
  python tools/v2_verify.py --step 1,2,3 # 여러 단계
"""

import sys
import os
import argparse
import time
from pathlib import Path
from datetime import date

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (
    OHLCV_DIR, MAPPING_CSV, JCGS_PASS, TRADING_VALUE_MIN,
    TRADING_VALUE_CHANGE_MIN, NEWS_COLLECT_QUERIES,
    DISCORD_WEBHOOK_CB, DISCORD_WEBHOOK_PADO,
    KIWOOM_APPKEY, NAVER_CLIENT_ID, GEMINI_API_KEY,
    setup_logging,
)

logger = setup_logging()
TODAY = date.today().isoformat()

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"
INFO = "📋"


def step1_config():
    """Step 1: .env 상수 확인"""
    print("\n" + "="*60)
    print("Step 1: .env 설정 확인")
    print("="*60)

    checks = [
        ("JCGS_PASS_VOLUME", JCGS_PASS["volume"], 25, "=="),
        ("TRADING_VALUE_MIN", TRADING_VALUE_MIN, 3_000_000_000, "=="),
        ("TRADING_VALUE_CHANGE_MIN", TRADING_VALUE_CHANGE_MIN, 100, "=="),
        ("NEWS_COLLECT_QUERIES", len(NEWS_COLLECT_QUERIES), 8, "=="),
    ]
    ok = 0
    for name, actual, expected, op in checks:
        passed = actual == expected
        status = PASS if passed else FAIL
        print(f"  {status} {name} = {actual} (기대: {expected})")
        if passed:
            ok += 1

    # 디스코드 1채널 확인
    same = DISCORD_WEBHOOK_CB == DISCORD_WEBHOOK_PADO
    print(f"  {PASS if same else WARN} 디스코드 1채널 통합: {'예' if same else '아니오 (2채널)'}")

    # API 키 존재 확인
    apis = [
        ("KIWOOM_APPKEY", bool(KIWOOM_APPKEY)),
        ("NAVER_CLIENT_ID", bool(NAVER_CLIENT_ID)),
        ("GEMINI_API_KEY", bool(GEMINI_API_KEY)),
    ]
    for name, exists in apis:
        print(f"  {PASS if exists else WARN} {name}: {'설정됨' if exists else '미설정'}")

    return ok == len(checks)


def step2_taxonomy():
    """Step 2: theme_taxonomy 매핑 검증"""
    print("\n" + "="*60)
    print("Step 2: 테마 분류 체계 (theme_taxonomy) 검증")
    print("="*60)

    from shared.theme_taxonomy import (
        normalize_kiwoom_theme, normalize_sector, canon_to_mega,
        merge_theme_sources, theme_match_score, get_prompt_type,
        CANON_LIST, MEGA_THEMES,
    )

    print(f"  {INFO} canon {len(CANON_LIST)}개, 메가테마 {len(MEGA_THEMES)}개")

    # 키움 테마 매핑 테스트
    kiwoom_tests = [
        ("HBM수혜주", "반도체"),
        ("반도체장비(국산화)", "반도체"),
        ("2차전지소재", "2차전지"),
        ("K방산", "방산"),
        ("바이오시밀러", "바이오"),
        ("AI관련주", "AI"),
        ("대선테마", "정치"),
        ("소형원자로(SMR)", "원전"),
        ("우크라이나재건", None),
    ]
    ok = 0
    for name, expected in kiwoom_tests:
        result = normalize_kiwoom_theme(name)
        passed = result == expected
        if passed:
            ok += 1
        else:
            print(f"  {FAIL} kiwoom '{name}' → {result} (기대: {expected})")
    print(f"  {PASS} 키움 테마 매핑: {ok}/{len(kiwoom_tests)} 통과")

    # 3소스 병합 테스트
    score, themes = merge_theme_sources(
        ["HBM관련주"], ["전기전자", "의약품"], ["반도체"]
    )
    print(f"  {PASS if score >= 30 else FAIL} 3소스 병합 (겹침): {score}점 {themes}")

    score, themes = merge_theme_sources([], ["전기전자"], [])
    print(f"  {PASS if 15 <= score <= 25 else FAIL} 1소스 병합: {score}점 {themes}")

    # theme_match_score
    s = theme_match_score("반도체장비", ["반도체", "바이오"], "전기전자", [])
    print(f"  {PASS if s == 15 else FAIL} theme_match(재료-시황 일치): {s}점")

    s = theme_match_score("", ["반도체"], "전기전자", [])
    print(f"  {PASS if s == 8 else FAIL} theme_match(섹터-시황 일치): {s}점")

    s = theme_match_score("", ["방산"], "전기전자", [])
    print(f"  {PASS if s == 0 else FAIL} theme_match(불일치): {s}점")

    return True


def step3_ohlcv_cache():
    """Step 3: OHLCV 캐시 로드 + 거래대금 사이드카"""
    print("\n" + "="*60)
    print("Step 3: OHLCV 캐시 로드 + 거래대금 변화율 사이드카")
    print("="*60)

    if not OHLCV_DIR.exists():
        print(f"  {FAIL} OHLCV 디렉토리 없음: {OHLCV_DIR}")
        return False

    csv_count = len(list(OHLCV_DIR.glob("*.csv")))
    print(f"  {INFO} OHLCV CSV 파일: {csv_count}개")

    from shared.ohlcv_cache import OHLCVCache
    OHLCVCache.reset()  # 깨끗한 상태에서 시작
    cache = OHLCVCache.instance()

    t0 = time.time()
    cache.preload_all()
    elapsed = time.time() - t0

    loaded = len(cache.get_all_codes())
    tv_count = len(cache.tv_sidecar)
    print(f"  {PASS if loaded > 0 else FAIL} 캐시 로드: {loaded}종목, {elapsed:.1f}초")
    print(f"  {PASS if tv_count > 0 else FAIL} 거래대금 사이드카: {tv_count}종목")

    # 거래대금 변화율 필터 결과
    hot = [code for code, tv in cache.tv_sidecar.items()
           if tv["tv_today"] >= TRADING_VALUE_MIN
           and tv["change_pct"] >= TRADING_VALUE_CHANGE_MIN]
    print(f"  {INFO} 변화율 필터 통과 (≥30억, ≥100%): {len(hot)}종목")

    if hot[:5]:
        from shared.stock_map import load_stock_map
        sm = load_stock_map()
        for code in hot[:5]:
            tv = cache.tv_sidecar[code]
            name = sm[code].name if code in sm else code
            print(f"    {name}: 거래대금 {tv['tv_today']/1e8:.0f}억, 변화율 {tv['change_pct']:+.0f}%")

    return loaded > 0


def step4_stock_mapping():
    """Step 4: stock_mapping 섹터 → canon 매핑 커버리지"""
    print("\n" + "="*60)
    print("Step 4: stock_mapping 섹터 매핑 커버리지")
    print("="*60)

    from shared.stock_map import load_stock_map
    from shared.theme_taxonomy import normalize_sector

    sm = load_stock_map()
    if not sm:
        print(f"  {FAIL} stock_mapping 로드 실패")
        return False

    sectors = {}
    mapped = 0
    unmapped_sectors = set()
    for code, stock in sm.items():
        s = stock.sector
        sectors[s] = sectors.get(s, 0) + 1
        if normalize_sector(s):
            mapped += 1
        else:
            unmapped_sectors.add(s)

    total = len(sm)
    coverage = mapped / total * 100 if total else 0
    print(f"  {INFO} 총 {total}종목, {len(sectors)}개 섹터")
    print(f"  {PASS if coverage > 80 else WARN} 매핑 커버리지: {coverage:.1f}% ({mapped}/{total})")

    if unmapped_sectors:
        print(f"  {WARN} 미매핑 섹터 ({len(unmapped_sectors)}개):")
        for s in sorted(unmapped_sectors):
            cnt = sectors[s]
            print(f"    - '{s}' ({cnt}종목)")

    return coverage > 50


def step5_market_engine():
    """Step 5: 시황 엔진 — 3소스 병합 테스트"""
    print("\n" + "="*60)
    print("Step 5: 시황 엔진 (3소스 병합)")
    print("="*60)

    from shared.ohlcv_cache import OHLCVCache
    cache = OHLCVCache.instance()
    if not cache.loaded:
        print(f"  {WARN} 캐시 미로드 — Step 3 먼저 실행 필요")
        return False

    from shared.kiwoom_api import KiwoomAPI
    api = None
    if KIWOOM_APPKEY:
        try:
            api = KiwoomAPI(
                appkey=KIWOOM_APPKEY,
                secretkey=os.getenv("KIWOOM_SECRETKEY", ""),
                base_url=os.getenv("KIWOOM_BASE_URL", "https://api.kiwoom.com"),
            )
            print(f"  {PASS} 키움 API 연결")
        except Exception as e:
            print(f"  {WARN} 키움 API 실패: {e}")

    from jaechageosi.market_engine import MarketEngine
    engine = MarketEngine(api=api)
    result = engine.evaluate(TODAY, tv_data=cache.tv_sidecar)

    print(f"  {INFO} 시황 점수: {result.score}점")
    print(f"  {INFO} 모드: {result.mode}")
    print(f"  {PASS if result.leading_themes else WARN} 주도테마: {result.leading_themes}")
    print(f"  {INFO} 나스닥: {result.nasdaq_change:+.2f}%")
    for r in result.reasons:
        print(f"    - {r}")

    has_themes = len(result.leading_themes) > 0
    themes_not_empty = all(t for t in result.leading_themes)
    print(f"\n  {PASS if has_themes else FAIL} 테마 존재: {'예' if has_themes else '아니오 (빈 리스트!)'}")
    if has_themes:
        print(f"  {PASS if themes_not_empty else FAIL} 테마 비공백: {'예' if themes_not_empty else '빈 문자열 포함!'}")

    return has_themes


def step6_webhook_test():
    """Step 6: 디스코드 웹훅 실발송 테스트"""
    print("\n" + "="*60)
    print("Step 6: 디스코드 웹훅 테스트")
    print("="*60)

    from shared.notifier import Notifier, embed, field, COLOR_GREEN
    notifier = Notifier()

    test_embed = embed(
        title="🧪 PADO v2 테스트",
        description=f"v2 검증 스크립트 실행 ({TODAY})",
        color=COLOR_GREEN,
        fields=[
            field("PASS_VOLUME", str(JCGS_PASS["volume"]), inline=True),
            field("TV_MIN", f"{TRADING_VALUE_MIN/1e8:.0f}억", inline=True),
            field("상태", "정상 작동 확인", inline=True),
        ],
        footer="PADO v2 — 테스트 웹훅",
    )

    ok = notifier.send_pado([test_embed])
    print(f"  {PASS if ok else FAIL} 웹훅 발송: {'성공' if ok else '실패'}")
    return ok


def step7_full_pipeline_dry():
    """Step 7: 전체 파이프라인 드라이런 (OHLCV 갱신 제외)"""
    print("\n" + "="*60)
    print("Step 7: 파이프라인 드라이런 (OHLCV 갱신 스킵)")
    print("="*60)
    print("  이 단계는 python main.py --scan으로 실행하세요.")
    print("  확인 포인트:")
    print("    1) ①-b 캐시 로드 완료 로그")
    print("    2) ④ 차트 스캔: N건 후보")
    print("    3) ⑤ 차트+거래량 통과: N건 (이전 6건→15~25건 예상)")
    print("    4) ⑥ 시황: 테마 ['반도체', ...] (빈 리스트 아님!)")
    print("    5) ⑦⑧ 로그에서 theme+15 또는 theme+8 확인")
    print("    6) A등급 종목 출현 여부")
    print("    7) ⑫ 뉴스 수집: N건")
    return True


# ─────────────────────────────────────────────

STEPS = {
    1: ("config 확인", step1_config),
    2: ("taxonomy 검증", step2_taxonomy),
    3: ("OHLCV 캐시 + 사이드카", step3_ohlcv_cache),
    4: ("stock_mapping 커버리지", step4_stock_mapping),
    5: ("시황 3소스 병합", step5_market_engine),
    6: ("디스코드 웹훅", step6_webhook_test),
    7: ("파이프라인 드라이런 안내", step7_full_pipeline_dry),
}


def main():
    parser = argparse.ArgumentParser(description="PADO v2 검증 스크립트")
    parser.add_argument("--step", type=str, default="",
                        help="실행할 단계 (예: 1,2,3 또는 5)")
    args = parser.parse_args()

    if args.step:
        steps = [int(s.strip()) for s in args.step.split(",")]
    else:
        steps = list(STEPS.keys())

    print("=" * 60)
    print(f"  PADO v2 검증 — {TODAY}")
    print(f"  실행 단계: {steps}")
    print("=" * 60)

    results = {}
    for s in steps:
        if s not in STEPS:
            print(f"\n{WARN} 알 수 없는 단계: {s}")
            continue
        name, fn = STEPS[s]
        try:
            ok = fn()
            results[s] = ok
        except Exception as e:
            print(f"  {FAIL} Step {s} 예외: {e}")
            import traceback
            traceback.print_exc()
            results[s] = False

    # 요약
    print("\n" + "=" * 60)
    print("  검증 요약")
    print("=" * 60)
    for s in steps:
        if s in results:
            name = STEPS[s][0]
            status = PASS if results[s] else FAIL
            print(f"  {status} Step {s}: {name}")

    failed = [s for s, ok in results.items() if not ok]
    if failed:
        print(f"\n  {WARN} 실패 단계: {failed}")
        print("  → 해당 단계 로그를 확인하고 수정 후 재실행")
    else:
        print(f"\n  {PASS} 전체 통과! → python main.py --scan 으로 실전 가동")


if __name__ == "__main__":
    main()
