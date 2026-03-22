"""
PADO v3 배포 전 검증 스크립트
==============================
python tools/v3_verify.py

키움 API 없이 실행 가능. 일요일 저녁에 돌려서 월요일 자동 운영 전 점검.
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PASS = "✅"
FAIL = "❌"
WARN = "⚠️"
results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    results.append((status, name, detail))
    print(f"  {status} {name}" + (f" — {detail}" if detail else ""))
    return condition


def warn(name, detail=""):
    results.append((WARN, name, detail))
    print(f"  {WARN} {name}" + (f" — {detail}" if detail else ""))


print("=" * 60)
print("PADO v3 배포 전 검증")
print("=" * 60)


# ── 1. 파일 존재 확인 ──
print("\n[1] 핵심 파일 존재")
ROOT = Path(__file__).resolve().parent.parent
must_exist = [
    "checkers/news_intelligence.py",
    "closingbell/screener.py",
    "shared/storage.py",
    "jaechageosi/market_engine.py",
    "jaechageosi/material_engine.py",
    "checkers/ai_analyzer.py",
    "main.py",
    "config.py",
]
for f in must_exist:
    check(f, (ROOT / f).exists())


# ── 2. import 검증 ──
print("\n[2] import 검증 (구문 에러 체크)")

try:
    from config import (CB_UNIVERSE_TOP_N, CB_MIN_PRICE, CB_MAX_PRICE,
                        CB_ETF_KEYWORDS, CB_OVERHEAT_RSI)
    check("config: CB 유니버스 설정",
          CB_UNIVERSE_TOP_N == 150 and CB_MIN_PRICE == 1000 and CB_MAX_PRICE == 150000,
          f"TOP_N={CB_UNIVERSE_TOP_N}, 가격={CB_MIN_PRICE}~{CB_MAX_PRICE}")
except Exception as e:
    check("config: CB 유니버스 설정", False, str(e))

try:
    from closingbell.screener import CBScreener
    check("closingbell.screener import", True)
    # _get_universe 메서드에 vol_stocks/val_stocks가 있는지 확인
    import inspect
    src = inspect.getsource(CBScreener._get_universe)
    has_volume = "get_volume_rank" in src
    has_value = "get_trading_value_rank" in src
    has_core = "core" in src
    check("CB 유니버스: 거래량+거래대금 2회 호출",
          has_volume and has_value,
          f"거래량={has_volume}, 거래대금={has_value}")
    check("CB 유니버스: core/fringe 태깅", has_core)
except Exception as e:
    check("closingbell.screener import", False, str(e))

try:
    from checkers.news_intelligence import (
        collect_google_news_rss, collect_naver_precision,
        detect_emerging_topics, run_news_collection, run_news_analysis,
        extract_active_words, match_stock_mentions,
        get_news_themes_for_market, get_related_news_for_stock,
    )
    check("news_intelligence import", True, "모든 함수 로드 성공")
except Exception as e:
    check("news_intelligence import", False, str(e))

try:
    from shared.storage import (
        save_news_v2_batch, get_news_v2_by_date, get_news_v2_count,
        save_news_analysis, get_news_analysis,
    )
    check("storage: news_v2 함수", True)
except Exception as e:
    check("storage: news_v2 함수", False, str(e))

try:
    from checkers.ai_analyzer import analyze_material
    import inspect
    sig = inspect.signature(analyze_material)
    params = list(sig.parameters.keys())
    check("ai_analyzer: related_news 파라미터",
          "related_news" in params and "emerging_keywords" in params,
          f"파라미터: {params}")
except Exception as e:
    check("ai_analyzer 파라미터", False, str(e))


# ── 3. DB 스키마 확인 ──
print("\n[3] DB 스키마 확인")
try:
    import sqlite3
    from config import APP_DB_PATH
    conn = sqlite3.connect(str(APP_DB_PATH))
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]

    check("news_v2 테이블", "news_v2" in tables)
    check("news_analysis 테이블", "news_analysis" in tables)
    check("news_daily 테이블 (레거시)", "news_daily" in tables)

    # news_v2 데이터 확인
    cnt = conn.execute("SELECT COUNT(*) FROM news_v2").fetchone()[0]
    check(f"news_v2 데이터: {cnt}건",
          cnt > 0,
          "어제 수집분이 있어야 내일 델타 감지 작동")

    # news_v2 컬럼 확인
    cols = [r[1] for r in conn.execute("PRAGMA table_info(news_v2)").fetchall()]
    must_cols = ["active_words", "stock_mentions", "first_sentence", "lang", "category"]
    missing = [c for c in must_cols if c not in cols]
    check("news_v2 컬럼 완전성",
          len(missing) == 0,
          f"missing: {missing}" if missing else f"컬럼 {len(cols)}개 OK")

    conn.close()
except Exception as e:
    check("DB 접근", False, str(e))


# ── 4. main.py 스케줄 확인 ──
print("\n[4] 스케줄 확인")
try:
    main_src = (ROOT / "main.py").read_text(encoding="utf-8")

    check("08:20 아침 뉴스 수집 스케줄",
          '"08:20"' in main_src and "_collect_morning_news" in main_src)

    check("_collect_morning_news 메서드 존재",
          "def _collect_morning_news" in main_src)

    check("⑫ 뉴스 v3 수집 (파이프라인)",
          "run_news_collection" in main_src)

    check("⑬ 뉴스 분석 (파이프라인)",
          "run_news_analysis" in main_src)

    check("--news CLI 옵션",
          '"--news"' in main_src)

    check("--news-analyze CLI 옵션",
          '"--news-analyze"' in main_src)
except Exception as e:
    check("main.py 파싱", False, str(e))


# ── 5. Google RSS 라이브 테스트 ──
print("\n[5] Google News RSS 라이브 테스트")
try:
    import feedparser
    check("feedparser 설치됨", True)
except ImportError:
    check("feedparser 설치됨", False, "pip install feedparser 필요")

try:
    from checkers.news_intelligence import _fetch_rss_google, extract_active_words
    items = _fetch_rss_google("코스피", lang="ko", max_items=5)
    check(f"한국어 RSS 수신: {len(items)}건", len(items) > 0)

    if items:
        title = items[0].get("title", "")
        words = extract_active_words(title, "", "ko")
        check(f"활성 단어 추출: {words[:5]}",
              len(words) > 0,
              f"제목: {title[:40]}")

    items_en = _fetch_rss_google("semiconductor", lang="en", max_items=5)
    check(f"영어 RSS 수신: {len(items_en)}건", len(items_en) > 0)

except Exception as e:
    check("RSS 테스트", False, str(e))


# ── 6. 활성 단어 품질 ──
print("\n[6] 활성 단어 품질")
try:
    from checkers.news_intelligence import extract_active_words, URL_NOISE, STOPWORDS_KO

    # 노이즈 체크
    test_title = "DAUM NET 연합뉴스 삼성전자 HBM 수주 1조원 돌파 급등"
    words = extract_active_words(test_title, "", "ko")
    check("DAUM/NET 노이즈 제거",
          "DAUM" not in words and "NET" not in words,
          f"결과: {words}")
    check("연합뉴스 노이즈 제거", "연합뉴스" not in words)
    check("삼성전자/HBM 유지",
          "삼성전자" in words or "HBM" in words,
          f"결과: {words}")
    check("급등/돌파 노이즈 제거",
          "급등" not in words and "돌파" not in words)
except Exception as e:
    check("활성 단어 품질", False, str(e))


# ── 7. OHLCV 경로 확인 ──
print("\n[7] OHLCV 데이터 확인")
try:
    from config import OHLCV_DIR
    ohlcv_path = Path(OHLCV_DIR)
    exists = ohlcv_path.exists()
    if exists:
        csv_count = len(list(ohlcv_path.glob("*.csv")))
        check(f"OHLCV 디렉토리: {csv_count}종목", csv_count > 100)
    else:
        warn(f"OHLCV 디렉토리 없음: {ohlcv_path}",
             "이 환경에서는 정상 (실제 PC에서 확인)")
except Exception as e:
    warn("OHLCV 확인 실패", str(e))


# ── 8. 델타 감지 시뮬레이션 ──
print("\n[8] 델타 감지 시뮬레이션")
try:
    from checkers.news_intelligence import detect_emerging_topics
    today = datetime.now().strftime("%Y-%m-%d")
    emerging = detect_emerging_topics(today)
    if emerging:
        check(f"델타 감지: {len(emerging)}건 부상 키워드",
              True,
              f"TOP3: {[e['word'] for e in emerging[:3]]}")
    else:
        warn("델타 감지: 부상 키워드 없음",
             "데이터 1일뿐이면 정상 (빈도 5 미만)")
except Exception as e:
    check("델타 감지", False, str(e))


# ── 결과 요약 ──
print("\n" + "=" * 60)
pass_count = sum(1 for s, _, _ in results if s == PASS)
fail_count = sum(1 for s, _, _ in results if s == FAIL)
warn_count = sum(1 for s, _, _ in results if s == WARN)

print(f"결과: {PASS} {pass_count}개 통과 / {FAIL} {fail_count}개 실패 / {WARN} {warn_count}개 경고")

if fail_count == 0:
    print("\n🎉 내일 자동 운영 준비 완료!")
    print("08:20 pado_daily.bat → 뉴스 수집 → 브리핑 → CB → 파이프라인")
else:
    print(f"\n🔧 {fail_count}개 항목 수정 필요:")
    for s, name, detail in results:
        if s == FAIL:
            print(f"  {FAIL} {name}: {detail}")

print("=" * 60)
