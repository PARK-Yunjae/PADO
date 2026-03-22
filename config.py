"""
PADO 통합 설정
==============
모든 운영 상수는 .env에서 관리. 코드 수정 없이 튜닝 가능.
"""

import os
import logging
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────────

def _env(key: str, default, type_fn=str):
    v = os.getenv(key, "")
    if not v:
        return default
    try:
        return type_fn(v)
    except (ValueError, TypeError):
        return default


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key, "").lower()
    if v in ("1", "true", "yes"):
        return True
    if v in ("0", "false", "no"):
        return False
    return default


def _env_path(key: str, default) -> Path:
    raw = os.getenv(key, "")
    return Path(raw) if raw else (default if isinstance(default, Path) else Path(default))


# ─────────────────────────────────────────────
# 경로
# ─────────────────────────────────────────────

PROJECT_DIR = Path(__file__).resolve().parent

DATA_DIR           = _env_path("DATA_DIR", "C:/Coding/data")
OHLCV_DIR          = _env_path("OHLCV_DIR", DATA_DIR / "ohlcv")
GLOBAL_CSV         = _env_path("GLOBAL_CSV", DATA_DIR / "global" / "global_merged.csv")
MAPPING_CSV        = _env_path("MAPPING_CSV", DATA_DIR / "stock_mapping.csv")
META_DIR           = _env_path("META_DIR", DATA_DIR / "meta")
MAJOR_HOLDER_CSV   = _env_path("MAJOR_HOLDER_CSV", META_DIR / "major_holder.csv")
COMPANY_PROFILE    = _env_path("COMPANY_PROFILE_CSV", META_DIR / "company_profile.csv")
FINANCIAL_SUMMARY  = _env_path("FINANCIAL_SUMMARY", META_DIR / "financial_summary.csv")

APP_DATA_DIR       = _env_path("APP_DATA_DIR", PROJECT_DIR / "data")
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)

APP_DB_PATH        = _env_path("APP_DB_PATH", APP_DATA_DIR / "pado.db")

REFERENCE_DIR      = APP_DATA_DIR / "reference"
REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR            = APP_DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

PERFORMANCE_DIR    = APP_DATA_DIR / "performance"
PERFORMANCE_DIR.mkdir(parents=True, exist_ok=True)

DART_CORP_MAP      = APP_DATA_DIR / "dart_corp_map.json"
KRX_HOLIDAYS       = REFERENCE_DIR / "krx_holidays.json"
MARKET_CALENDAR    = REFERENCE_DIR / "market_calendar.json"
SAFETY_BLOCKLIST   = REFERENCE_DIR / "safety_blocklist.json"

# ─────────────────────────────────────────────
# API 키
# ─────────────────────────────────────────────

KIWOOM_BASE_URL      = _env("KIWOOM_BASE_URL", "https://api.kiwoom.com")
KIWOOM_APPKEY        = _env("KIWOOM_APPKEY", "")
KIWOOM_SECRETKEY     = _env("KIWOOM_SECRETKEY", "")

DART_API_KEY         = _env("DART_API_KEY", "")
NAVER_CLIENT_ID      = _env("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET  = _env("NAVER_CLIENT_SECRET", "")
GEMINI_API_KEY       = _env("GEMINI_API_KEY", "")
GEMINI_MODEL         = _env("GEMINI_MODEL", "gemini-2.5-flash-lite")

# ─────────────────────────────────────────────
# 디스코드 웹훅 (1채널 통합)
# ─────────────────────────────────────────────

DISCORD_WEBHOOK      = _env("DISCORD_WEBHOOK", "")
DISCORD_WEBHOOK_CB   = DISCORD_WEBHOOK
DISCORD_WEBHOOK_PADO = DISCORD_WEBHOOK

# ─────────────────────────────────────────────
# API 호출 제한
# ─────────────────────────────────────────────

API_SLEEP_KIWOOM     = _env("API_SLEEP_KIWOOM", 0.3, float)
API_SLEEP_DART       = _env("API_SLEEP_DART", 0.5, float)
API_SLEEP_NAVER      = _env("API_SLEEP_NAVER", 0.2, float)
API_SLEEP_GEMINI     = _env("API_SLEEP_GEMINI", 1.0, float)
API_TIMEOUT          = _env("API_TIMEOUT", 10, int)
API_MAX_RETRY        = _env("API_MAX_RETRY", 2, int)
MAX_MATERIAL_EVAL    = _env("MAX_MATERIAL_EVAL", 25, int)

# ─────────────────────────────────────────────
# 스캔 필터
# ─────────────────────────────────────────────

SCAN_MIN_SIGNALS     = _env("SCAN_MIN_SIGNALS", 1, int)
SCAN_MIN_SCORE       = _env("SCAN_MIN_SCORE", 25, int)
SCAN_MAX_CANDIDATES  = _env("SCAN_MAX_CANDIDATES", 60, int)

# ─────────────────────────────────────────────
# ClosingBell 스코어링
# ─────────────────────────────────────────────

CB_SCORE_RSI         = _env("CB_SCORE_RSI", 20.0, float)
CB_SCORE_MA_ALIGN    = _env("CB_SCORE_MA_ALIGN", 20.0, float)
CB_SCORE_CHANGE      = _env("CB_SCORE_CHANGE", 15.0, float)
CB_SCORE_VOL_BURST   = _env("CB_SCORE_VOL_BURST", 15.0, float)
CB_SCORE_BROKER      = _env("CB_SCORE_BROKER", 10.0, float)
CB_SCORE_SHORT       = _env("CB_SCORE_SHORT", 10.0, float)
CB_SCORE_GC_BONUS    = _env("CB_SCORE_GC_BONUS", 10.0, float)
CB_SCORE_GGE_BONUS   = _env("CB_SCORE_GGE_BONUS", 5.0, float)
CB_SCORE_OBV_BONUS   = _env("CB_SCORE_OBV_BONUS", 3.0, float)
CB_OVERHEAT_RSI      = _env("CB_OVERHEAT_RSI", 75.0, float)

# ClosingBell 유니버스
CB_UNIVERSE_TOP_N    = _env("CB_UNIVERSE_TOP_N", 150, int)
CB_MIN_PRICE         = _env("CB_MIN_PRICE", 1000, int)
CB_MAX_PRICE         = _env("CB_MAX_PRICE", 150000, int)
CB_ETF_KEYWORDS      = ["ETF", "ETN", "KODEX", "TIGER", "KBSTAR", "ARIRANG",
                        "HANARO", "SOL", "스팩", "SPAC", "리츠"]

# ─────────────────────────────────────────────
# 재차거시 threshold / 가중치
# ─────────────────────────────────────────────

JCGS_PASS = {
    "chart":    _env("JCGS_PASS_CHART", 40, int),
    "volume":   _env("JCGS_PASS_VOLUME", 25, int),    # v2: 35→25 (ka10045 미구현)
    "material": _env("JCGS_PASS_MATERIAL", 50, int),
    "market":   _env("JCGS_PASS_MARKET", 40, int),
}

JCGS_FAIL = {
    "chart":    _env("JCGS_FAIL_CHART", 10, int),
    "volume":   _env("JCGS_FAIL_VOLUME", 5, int),
    "material": _env("JCGS_FAIL_MATERIAL", 20, int),
    "market":   _env("JCGS_FAIL_MARKET", 15, int),
}

JCGS_WEIGHT = {
    "chart":    _env("JCGS_WEIGHT_CHART", 0.30, float),
    "volume":   _env("JCGS_WEIGHT_VOLUME", 0.30, float),
    "material": _env("JCGS_WEIGHT_MATERIAL", 0.20, float),
    "market":   _env("JCGS_WEIGHT_MARKET", 0.20, float),
}

# ─────────────────────────────────────────────
# 시황 배점
# ─────────────────────────────────────────────

MARKET_SCORE_THEME     = _env("MARKET_SCORE_THEME", 35, int)
MARKET_SCORE_CALENDAR  = _env("MARKET_SCORE_CALENDAR", 25, int)
MARKET_SCORE_NASDAQ    = _env("MARKET_SCORE_NASDAQ", 20, int)
MARKET_SCORE_KOSPI_GAP = _env("MARKET_SCORE_KOSPI_GAP", 20, int)

# ─────────────────────────────────────────────
# 감시 설정
# ─────────────────────────────────────────────

WATCHLIST_MAX_HOLD_DAYS = _env("WATCHLIST_MAX_HOLD_DAYS", 20, int)
WATCHLIST_MAX_ITEMS     = _env("WATCHLIST_MAX_ITEMS", 10, int)

# ─────────────────────────────────────────────
# 뉴스 설정
# ─────────────────────────────────────────────

NEWS_SEARCH_COUNT   = _env("NEWS_SEARCH_COUNT", 5, int)
NEWS_SEARCH_FEATURE = _env_bool("NEWS_SEARCH_FEATURE", True)
NEWS_MAX_AGE_DAYS   = _env("NEWS_MAX_AGE_DAYS", 7, int)

# ─────────────────────────────────────────────
# v2: 거래대금 변화율 (시황)
# ─────────────────────────────────────────────

TRADING_VALUE_MIN        = _env("TRADING_VALUE_MIN", 3_000_000_000, int)   # 하한 30억
TRADING_VALUE_CHANGE_MIN = _env("TRADING_VALUE_CHANGE_MIN", 100, int)      # 변화율 100%
TRADING_VALUE_MA_DAYS    = _env("TRADING_VALUE_MA_DAYS", 20, int)          # 평균 기간

# ─────────────────────────────────────────────
# v2: 뉴스 매일 수집
# ─────────────────────────────────────────────

def _env_list(key: str, default: list) -> list:
    v = os.getenv(key, "")
    if not v:
        return default
    return [x.strip() for x in v.split(",") if x.strip()]

NEWS_COLLECT_QUERIES   = _env_list(
    "NEWS_COLLECT_QUERIES",
    ["코스피", "코스닥", "주식시장", "반도체", "2차전지", "바이오",
     "인공지능", "방산", "원전", "로봇", "조선", "금융", "대선", "경제정책", "수출"],
)
NEWS_COLLECT_PER_QUERY = _env("NEWS_COLLECT_PER_QUERY", 30, int)

# 기대감 증폭 키워드 (유목민 3권 p.4970)
AMPLIFIER_KEYWORDS = {
    "규모": ["세계 최초", "국내 최초", "사상 최대", "최대 수혜", "최대 흥행"],
    "순위": ["1위", "시장 점유율", "세계 1위", "아마존 1위", "구글스토어 1위"],
    "대기업": ["테슬라", "애플", "구글", "아마존", "삼성전자", "엔비디아"],
    "계약": ["기술 수출", "수천억 계약", "글로벌 제휴", "대규모 수주"],
    "정책": ["정부 육성", "수십 조 투자", "규제 완화", "오일머니", "사우디"],
    "특수": ["반사이익", "완전관해", "국산화 성공", "정관 변경"],
}

# ─────────────────────────────────────────────
# 로깅
# ─────────────────────────────────────────────

LOG_LEVEL = _env("LOG_LEVEL", "INFO")

# ─────────────────────────────────────────────
# ClosingBell 호환 별명 (CB 복사 파일이 참조하는 변수)
# ─────────────────────────────────────────────

COMPANY_PROFILE_CSV = COMPANY_PROFILE
API_DELAY           = API_SLEEP_KIWOOM
TRADING_CALENDAR_REFERENCE_CODE = _env("TRADING_CALENDAR_REFERENCE_CODE", "005930")

# market_context.py 호환
FOMC_PENALTY            = _env("FOMC_PENALTY", -3.0, float)
HOLDER_DUMP_THRESHOLD   = _env("HOLDER_DUMP_THRESHOLD", -10.0, float)
HOLDER_LOW_PENALTY      = _env("HOLDER_LOW_PENALTY", -2.0, float)
HOLDER_LOW_PRICE_MAX    = _env("HOLDER_LOW_PRICE_MAX", 3000, int)
HOLDER_LOW_THRESH       = _env("HOLDER_LOW_THRESH", 5.0, float)
MARKET_CALENDAR_PATH    = MARKET_CALENDAR
POLITICAL_CRISIS_MODE   = _env_bool("POLITICAL_CRISIS_MODE", False)

# performance_tracker.py 호환
PERFORMANCE_TRACK_DAYS  = _env("PERFORMANCE_TRACK_DAYS", 5, int)


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("pado")
    if logger.handlers:
        return logger
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False  # 루트 로거 전파 방지 (중복 출력 차단)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    fh = TimedRotatingFileHandler(
        LOG_DIR / "pado.log", when="midnight", backupCount=30, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    return logger
