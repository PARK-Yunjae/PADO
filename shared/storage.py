"""
PADO 통합 DB (pado.db)
======================
SQLite WAL 모드 + 인덱스. CB 테이블 + PADO 테이블 공존.
"""

import json
import sqlite3
import zlib
from datetime import datetime, timezone

from config import APP_DB_PATH, setup_logging

logger = setup_logging().getChild("storage")

# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _encode(data) -> bytes:
    return zlib.compress(json.dumps(data, ensure_ascii=False, default=str).encode())


def _decode(blob) -> dict | list | None:
    if blob is None:
        return None
    raw = bytes(blob) if isinstance(blob, memoryview) else blob
    return json.loads(zlib.decompress(raw))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(APP_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.row_factory = sqlite3.Row
    return conn

# ─────────────────────────────────────────────
# 초기화
# ─────────────────────────────────────────────

_TABLES = """
-- ClosingBell 모듈
CREATE TABLE IF NOT EXISTS cb_screen_runs (
    run_date    TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    payload     BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS cb_watchlist (
    created     TEXT PRIMARY KEY,
    expires     TEXT,
    updated_at  TEXT NOT NULL,
    payload     BLOB NOT NULL
);

-- 파동 모듈
CREATE TABLE IF NOT EXISTS wave_signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    code        TEXT NOT NULL,
    name        TEXT,
    wave_type   TEXT NOT NULL,
    detect_date TEXT NOT NULL,
    strength    REAL,
    wave_count  INTEGER DEFAULT 1,
    notified    INTEGER DEFAULT 0,
    created_at  TEXT NOT NULL,
    UNIQUE(code, wave_type, detect_date)
);

-- 재차거시 스캔 결과
CREATE TABLE IF NOT EXISTS jcgs_scan_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date       TEXT NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT,
    grade           TEXT NOT NULL,
    confidence      INTEGER,
    action          TEXT,
    chart_state     TEXT,
    flow_state      TEXT,
    chart_score     INTEGER,
    volume_score    INTEGER,
    material_score  INTEGER,
    market_score    INTEGER,
    theme_match     INTEGER DEFAULT 0,
    synergy         INTEGER DEFAULT 0,
    reject_reason   TEXT,
    signal_type         TEXT,
    recommended_hold_days TEXT,
    strategy_bucket     TEXT,
    payload         BLOB,
    created_at      TEXT NOT NULL,
    UNIQUE(scan_date, code)
);

-- 재차거시 감시명부
CREATE TABLE IF NOT EXISTS jcgs_watchlist (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT NOT NULL,
    name            TEXT,
    grade           TEXT NOT NULL,
    added_date      TEXT NOT NULL,
    source          TEXT,
    status          TEXT DEFAULT 'watching',
    scan_result_id  INTEGER,
    entered_at      TEXT,
    exited_at       TEXT,
    entry_reason    TEXT,
    exit_reason     TEXT,
    entry_price     REAL,
    stop_loss       REAL,
    target_price    REAL,
    max_hold_days   INTEGER DEFAULT 20,
    updated_at      TEXT NOT NULL,
    UNIQUE(code, added_date)
);

-- 성과 추적
CREATE TABLE IF NOT EXISTS performance (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    code            TEXT NOT NULL,
    name            TEXT,
    source          TEXT NOT NULL,
    signal_date     TEXT NOT NULL,
    grade           TEXT,
    scan_result_id  INTEGER,
    track_day       INTEGER NOT NULL,
    track_date      TEXT NOT NULL,
    entry_price     REAL,
    current_price   REAL,
    return_pct      REAL,
    updated_at      TEXT NOT NULL
);

-- 웹훅 이력
CREATE TABLE IF NOT EXISTS notifications (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,
    ref_date    TEXT,
    sent_at     TEXT NOT NULL,
    status      TEXT NOT NULL,
    payload     BLOB
);

-- 시황 일별
CREATE TABLE IF NOT EXISTS market_daily (
    date            TEXT PRIMARY KEY,
    mode            TEXT,
    leading_themes  TEXT,
    nasdaq_chg      REAL,
    kospi_chg       REAL,
    kosdaq_chg      REAL,
    dangerous       INTEGER DEFAULT 0,
    score           INTEGER,
    created_at      TEXT NOT NULL
);

-- v2: 뉴스 매일 축적 (레거시, 하위 호환)
CREATE TABLE IF NOT EXISTS news_daily (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    collect_date TEXT NOT NULL,
    title        TEXT NOT NULL,
    snippet      TEXT,
    source       TEXT,
    pub_date     TEXT,
    link         TEXT,
    created_at   TEXT NOT NULL
);

-- v3: CB 감시종목 (이전 ClosingBell 방식: 감시 → 눌림목 시 알림)
CREATE TABLE IF NOT EXISTS cb_watch_stocks (
    code        TEXT NOT NULL,
    name        TEXT,
    score       REAL,
    rsi         REAL,
    alignment   TEXT,
    pool_type   TEXT,
    reasons     TEXT,
    added_date  TEXT NOT NULL,
    status      TEXT DEFAULT 'watching',
    UNIQUE(code, added_date)
);

-- v3: 뉴스 인텔리전스
CREATE TABLE IF NOT EXISTS news_v2 (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    collect_date    TEXT NOT NULL,
    source          TEXT NOT NULL,
    category        TEXT,
    query           TEXT,
    title           TEXT NOT NULL,
    first_sentence  TEXT,
    snippet         TEXT,
    link            TEXT,
    pub_date        TEXT,
    publisher       TEXT,
    active_words    TEXT,
    stock_mentions  TEXT,
    lang            TEXT DEFAULT 'ko',
    created_at      TEXT DEFAULT (datetime('now','localtime'))
);

-- v3: 뉴스 분석 결과 (델타 감지 + Gemini)
CREATE TABLE IF NOT EXISTS news_analysis (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_date   TEXT NOT NULL,
    payload         BLOB,
    created_at      TEXT DEFAULT (datetime('now','localtime'))
);

-- v5: 눌림목 시그널 + 3차 검증 결과 (승률 분석용)
CREATE TABLE IF NOT EXISTS pullback_signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_date     TEXT NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT,
    d_plus          INTEGER,
    explosion_date  TEXT,
    explosion_ratio REAL,
    vol_remain_pct  REAL,
    signal_strength INTEGER,
    ma_touch        TEXT,
    is_bearish      INTEGER DEFAULT 0,
    entry_price     REAL,
    stop_loss       REAL,
    target_price    REAL,
    verdict         TEXT,
    verify_reasons  TEXT,
    dart_result     TEXT,
    short_ratio     REAL,
    foreign_net     INTEGER,
    inst_net        INTEGER,
    d1_return       REAL,
    d2_return       REAL,
    d3_return       REAL,
    d5_return       REAL,
    created_at      TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(signal_date, code)
);
"""

_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_wave_date ON wave_signals(detect_date);
CREATE INDEX IF NOT EXISTS idx_jcgs_scan_date ON jcgs_scan_results(scan_date);
CREATE INDEX IF NOT EXISTS idx_jcgs_scan_code ON jcgs_scan_results(code);
CREATE INDEX IF NOT EXISTS idx_jcgs_wl_status ON jcgs_watchlist(status);
CREATE INDEX IF NOT EXISTS idx_jcgs_wl_code ON jcgs_watchlist(code);
CREATE INDEX IF NOT EXISTS idx_perf_source ON performance(source, signal_date);
CREATE INDEX IF NOT EXISTS idx_news_date ON news_daily(collect_date);
CREATE INDEX IF NOT EXISTS idx_news_title ON news_daily(title);
CREATE INDEX IF NOT EXISTS idx_news_v2_date ON news_v2(collect_date);
CREATE INDEX IF NOT EXISTS idx_news_v2_category ON news_v2(category);
CREATE INDEX IF NOT EXISTS idx_news_v2_source ON news_v2(source);
CREATE INDEX IF NOT EXISTS idx_news_analysis_date ON news_analysis(analysis_date);
CREATE INDEX IF NOT EXISTS idx_cb_watch_status ON cb_watch_stocks(status);
CREATE INDEX IF NOT EXISTS idx_cb_watch_date ON cb_watch_stocks(added_date);
CREATE INDEX IF NOT EXISTS idx_pullback_date ON pullback_signals(signal_date);
CREATE INDEX IF NOT EXISTS idx_pullback_code ON pullback_signals(code);
CREATE INDEX IF NOT EXISTS idx_pullback_verdict ON pullback_signals(verdict);
"""


def init_storage():
    """DB 초기화 — 테이블 + 인덱스 생성."""
    conn = _connect()
    conn.executescript(_TABLES)
    conn.executescript(_INDEXES)

    # v4.1 마이그레이션: signal_type 컬럼 추가
    try:
        conn.execute("SELECT signal_type FROM jcgs_scan_results LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE jcgs_scan_results ADD COLUMN signal_type TEXT")
        conn.execute("ALTER TABLE jcgs_scan_results ADD COLUMN recommended_hold_days TEXT")
        conn.execute("ALTER TABLE jcgs_scan_results ADD COLUMN strategy_bucket TEXT")
        conn.commit()
        logger.info("v4.1 마이그레이션: signal_type 컬럼 추가")

    conn.close()
    logger.info("pado.db 초기화 완료")


# ─────────────────────────────────────────────
# 재차거시 스캔 결과
# ─────────────────────────────────────────────

def save_scan_result(row: dict) -> int | None:
    conn = _connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO jcgs_scan_results
               (scan_date,code,name,grade,confidence,action,
                chart_state,flow_state,chart_score,volume_score,
                material_score,market_score,theme_match,synergy,
                reject_reason,signal_type,recommended_hold_days,
                strategy_bucket,payload,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["scan_date"], row["code"], row.get("name"),
             row["grade"], row.get("confidence"), row.get("action"),
             row.get("chart_state"), row.get("flow_state"),
             row.get("chart_score"), row.get("volume_score"),
             row.get("material_score"), row.get("market_score"),
             row.get("theme_match", 0), row.get("synergy", 0),
             row.get("reject_reason"),
             row.get("signal_type", "score_only"),
             row.get("recommended_hold_days", "D+3~5"),
             row.get("strategy_bucket", "pullback"),
             _encode(row) if row else None, _now()),
        )
        conn.commit()
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    except Exception as e:
        logger.error(f"save_scan_result 실패: {e}")
        return None
    finally:
        conn.close()


def get_scan_results(date: str) -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM jcgs_scan_results WHERE scan_date=? ORDER BY confidence DESC", (date,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# 감시명부
# ─────────────────────────────────────────────

def add_watchlist(row: dict) -> None:
    conn = _connect()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO jcgs_watchlist
               (code,name,grade,added_date,source,status,
                scan_result_id,entry_price,stop_loss,target_price,
                max_hold_days,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["code"], row.get("name"), row["grade"],
             row["added_date"], row.get("source", "scan"), "watching",
             row.get("scan_result_id"), row.get("entry_price"),
             row.get("stop_loss"), row.get("target_price"),
             row.get("max_hold_days", 20), _now()),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"add_watchlist 실패: {e}")
    finally:
        conn.close()


def get_watching() -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM jcgs_watchlist WHERE status='watching' ORDER BY added_date"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_watchlist_status(code: str, status: str, **kwargs) -> None:
    conn = _connect()
    sets = ["status=?", "updated_at=?"]
    vals = [status, _now()]
    for k, v in kwargs.items():
        sets.append(f"{k}=?")
        vals.append(v)
    vals.append(code)
    conn.execute(
        f"UPDATE jcgs_watchlist SET {','.join(sets)} WHERE code=? AND status='watching'",
        vals,
    )
    conn.commit()
    conn.close()


def expire_old_watchlist(max_days: int = 20) -> int:
    conn = _connect()
    cur = conn.execute(
        """UPDATE jcgs_watchlist SET status='expired', exit_reason='max_hold_days',
                  exited_at=?, updated_at=?
           WHERE status='watching'
             AND julianday('now') - julianday(added_date) > ?""",
        (_now(), _now(), max_days),
    )
    conn.commit()
    count = cur.rowcount
    conn.close()
    return count


# ─────────────────────────────────────────────
# 파동
# ─────────────────────────────────────────────

def save_wave_signal(row: dict) -> None:
    conn = _connect()
    try:
        conn.execute(
            """INSERT OR IGNORE INTO wave_signals
               (code,name,wave_type,detect_date,strength,wave_count,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (row["code"], row.get("name"), row["wave_type"],
             row["detect_date"], row.get("strength"),
             row.get("wave_count", 1), _now()),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"save_wave_signal 실패: {e}")
    finally:
        conn.close()


def get_wave_signals(date: str) -> list[dict]:
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM wave_signals WHERE detect_date=?", (date,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_wave_count(code: str, wave_type: str) -> int:
    """이 종목의 이전 파동 횟수 조회 (약화 판정용)."""
    conn = _connect()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM wave_signals WHERE code=? AND wave_type=?",
        (code, wave_type),
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


# ─────────────────────────────────────────────
# 시황 일별
# ─────────────────────────────────────────────

def save_market_daily(row: dict) -> None:
    conn = _connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO market_daily
               (date,mode,leading_themes,nasdaq_chg,kospi_chg,kosdaq_chg,
                dangerous,score,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (row["date"], row.get("mode"),
             json.dumps(row.get("leading_themes", []), ensure_ascii=False),
             row.get("nasdaq_chg"), row.get("kospi_chg"), row.get("kosdaq_chg"),
             1 if row.get("dangerous") else 0, row.get("score"), _now()),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"save_market_daily 실패: {e}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# v2: 뉴스 매일 축적
# ─────────────────────────────────────────────

def save_news_batch(rows: list[dict]) -> None:
    """뉴스 일괄 저장."""
    conn = _connect()
    try:
        conn.executemany(
            """INSERT INTO news_daily (collect_date,title,snippet,source,pub_date,link,created_at)
               VALUES (?,?,?,?,?,?,?)""",
            [(r["collect_date"], r["title"], r.get("snippet", ""),
              r.get("source", ""), r.get("pub_date", ""), r.get("link", ""),
              _now()) for r in rows],
        )
        conn.commit()
    except Exception as e:
        logger.error(f"save_news_batch 실패: {e}")
    finally:
        conn.close()


def get_today_news(date: str) -> list[dict]:
    """특정 날짜의 뉴스 조회."""
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM news_daily WHERE collect_date=?", (date,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────
# 웹훅 이력
# ─────────────────────────────────────────────

def save_notification(event_type: str, ref_date: str, status: str, payload=None) -> None:
    conn = _connect()
    conn.execute(
        """INSERT INTO notifications (event_type,ref_date,sent_at,status,payload)
           VALUES (?,?,?,?,?)""",
        (event_type, ref_date, _now(), status, _encode(payload) if payload else None),
    )
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# CB 호환 (ClosingBell 모듈용)
# ─────────────────────────────────────────────

def save_cb_screen(run_date: str, payload: dict) -> None:
    conn = _connect()
    conn.execute(
        "INSERT OR REPLACE INTO cb_screen_runs (run_date,created_at,payload) VALUES (?,?,?)",
        (run_date, _now(), _encode(payload)),
    )
    conn.commit()
    conn.close()


def get_cb_screen(run_date: str) -> dict | None:
    conn = _connect()
    row = conn.execute("SELECT payload FROM cb_screen_runs WHERE run_date=?", (run_date,)).fetchone()
    conn.close()
    return _decode(row["payload"]) if row else None


# ─────────────────────────────────────────────
# v3: 뉴스 인텔리전스
# ─────────────────────────────────────────────

def save_news_v2_batch(rows: list[dict]) -> None:
    """news_v2 일괄 저장."""
    conn = _connect()
    try:
        conn.executemany(
            """INSERT INTO news_v2
               (collect_date, source, category, query, title, first_sentence,
                snippet, link, pub_date, publisher, active_words, stock_mentions, lang)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(r["collect_date"], r["source"], r.get("category", ""),
              r.get("query", ""), r["title"], r.get("first_sentence", ""),
              r.get("snippet", ""), r.get("link", ""), r.get("pub_date", ""),
              r.get("publisher", ""),
              r.get("active_words", "[]"), r.get("stock_mentions", "[]"),
              r.get("lang", "ko"))
             for r in rows],
        )
        conn.commit()
    except Exception as e:
        logger.error(f"save_news_v2_batch 실패: {e}")
    finally:
        conn.close()


def get_news_v2_by_date(date: str) -> list[dict]:
    """특정 날짜의 news_v2 조회."""
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM news_v2 WHERE collect_date=?", (date,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_news_v2_count(date: str) -> int:
    """특정 날짜 news_v2 건수."""
    conn = _connect()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM news_v2 WHERE collect_date=?", (date,)
    ).fetchone()
    conn.close()
    return row["cnt"] if row else 0


def save_news_analysis(date: str, payload: dict) -> None:
    """뉴스 분석 결과 저장 (델타 + Gemini)."""
    conn = _connect()
    conn.execute(
        "INSERT INTO news_analysis (analysis_date, payload) VALUES (?,?)",
        (date, _encode(payload)),
    )
    conn.commit()
    conn.close()


def get_news_analysis(date: str) -> dict | None:
    """뉴스 분석 결과 조회."""
    conn = _connect()
    row = conn.execute(
        "SELECT payload FROM news_analysis WHERE analysis_date=? ORDER BY id DESC LIMIT 1",
        (date,)
    ).fetchone()
    conn.close()
    return _decode(row["payload"]) if row else None


# ─────────────────────────────────────────────
# v3: CB 감시종목 (이전 ClosingBell 방식)
# ─────────────────────────────────────────────

def save_cb_watch(stocks: list[dict], date: str) -> int:
    """CB TOP5를 감시종목으로 등록."""
    conn = _connect()
    saved = 0
    for s in stocks:
        try:
            conn.execute(
                """INSERT OR REPLACE INTO cb_watch_stocks
                   (code,name,score,rsi,alignment,pool_type,reasons,added_date,status)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (s["code"], s.get("name", ""), s.get("score", 0),
                 s.get("rsi", 0), s.get("alignment", ""),
                 s.get("pool_type", ""), str(s.get("reasons", [])),
                 date, "watching"),
            )
            saved += 1
        except Exception as e:
            logger.debug(f"CB 감시 저장 실패 {s.get('code')}: {e}")
    conn.commit()
    conn.close()
    return saved


def get_cb_watching() -> list[dict]:
    """CB 감시 중인 종목 조회."""
    conn = _connect()
    rows = conn.execute(
        "SELECT * FROM cb_watch_stocks WHERE status='watching' ORDER BY score DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def expire_cb_watch(max_days: int = 10) -> int:
    """오래된 CB 감시종목 만료 처리."""
    conn = _connect()
    result = conn.execute(
        """UPDATE cb_watch_stocks SET status='expired'
           WHERE status='watching'
           AND julianday('now') - julianday(added_date) > ?""",
        (max_days,),
    )
    conn.commit()
    expired = result.rowcount
    conn.close()
    return expired


# ─────────────────────────────────────────────
# 성과 추적 (performance_tracker.py용)
# ─────────────────────────────────────────────

def iter_screen_results() -> list[dict]:
    """CB 스크린 결과 전체 순회. payload에서 date, top(종목), skipped 등 추출."""
    conn = _connect()
    rows = conn.execute(
        "SELECT run_date, payload FROM cb_screen_runs ORDER BY run_date"
    ).fetchall()
    conn.close()

    results = []
    for row in rows:
        payload = _decode(row["payload"])
        if payload is None:
            continue
        data = {"date": row["run_date"]}
        if isinstance(payload, dict):
            data["top"] = [
                {**s, "price": s.get("price", s.get("close", 0)),
                 "rank": i + 1, "score": s.get("score", 0)}
                for i, s in enumerate(payload.get("stocks", []))
            ]
            data["skipped"] = payload.get("skipped", False)
        results.append(data)
    return results


def get_buy_picks(pick_date: str) -> dict | None:
    """특정 날짜의 매수 후보 (CB 스크린 결과 기반)."""
    conn = _connect()
    row = conn.execute(
        "SELECT payload FROM cb_screen_runs WHERE run_date=?", (pick_date,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    payload = _decode(row["payload"])
    if not payload or not isinstance(payload, dict):
        return None
    picks = [
        {**s, "current_price": s.get("price", s.get("close", 0))}
        for s in payload.get("stocks", [])
    ]
    return {"picks": picks}


def list_buy_pick_dates(desc: bool = True) -> list[str]:
    """매수 후보가 저장된 날짜 목록."""
    order = "DESC" if desc else "ASC"
    conn = _connect()
    rows = conn.execute(
        f"SELECT run_date FROM cb_screen_runs ORDER BY run_date {order}"
    ).fetchall()
    conn.close()
    return [r["run_date"] for r in rows]


def save_buy_pick_outcomes(records: list[dict]) -> int:
    """성과 추적 결과를 performance 테이블에 저장."""
    if not records:
        return 0
    conn = _connect()
    saved = 0
    for rec in records:
        try:
            conn.execute(
                """INSERT OR REPLACE INTO performance
                   (code, name, source, signal_date, grade,
                    track_day, track_date, entry_price, current_price,
                    return_pct, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    rec.get("code", ""),
                    rec.get("name", ""),
                    rec.get("signal_type", "cb_screen"),
                    rec.get("pick_date", rec.get("signal_date", "")),
                    rec.get("conviction", ""),
                    rec.get("track_day", 0),
                    rec.get("track_date", ""),
                    rec.get("buy_price", 0),
                    rec.get("track_price", 0),
                    rec.get("return_pct", 0),
                    _now(),
                ),
            )
            saved += 1
        except Exception as e:
            logger.debug(f"save_buy_pick_outcome 실패 {rec.get('code')}: {e}")
    conn.commit()
    conn.close()
    return saved


def update_pick_snapshot_returns(records: list[dict]) -> None:
    """기존 performance 레코드의 수익률 갱신 (최신 OHLCV 기반 재계산 시)."""
    if not records:
        return
    conn = _connect()
    for rec in records:
        try:
            conn.execute(
                """UPDATE performance SET current_price=?, return_pct=?, updated_at=?
                   WHERE code=? AND signal_date=? AND track_day=?""",
                (
                    rec.get("track_price", 0),
                    rec.get("return_pct", 0),
                    _now(),
                    rec.get("code", ""),
                    rec.get("pick_date", rec.get("signal_date", "")),
                    rec.get("track_day", 0),
                ),
            )
        except Exception:
            pass
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────
# v5: 눌림목 시그널 + 3차 검증 결과 저장
# ─────────────────────────────────────────────

def save_pullback_signal(signal_date: str, hit: dict, verdict: dict) -> bool:
    """눌림목 시그널 + 3차 검증 결과를 DB에 저장 (승률 분석용).

    Args:
        signal_date: 시그널 발생일 (YYYY-MM-DD)
        hit: check_pullbacks() 결과 dict
        verdict: _verify_pullback() 결과 {"grade": ..., "reasons": [...]}
    """
    conn = _connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO pullback_signals
               (signal_date, code, name, d_plus, explosion_date, explosion_ratio,
                vol_remain_pct, signal_strength, ma_touch, is_bearish,
                entry_price, stop_loss, target_price,
                verdict, verify_reasons)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                signal_date,
                hit.get("code", ""),
                hit.get("name", ""),
                hit.get("d_plus", 0),
                hit.get("explosion_date", ""),
                hit.get("explosion_ratio", 0),
                hit.get("vol_ratio_pct", 0),
                hit.get("signal_strength", 0),
                hit.get("ma_touch", ""),
                1 if "음봉" in hit.get("note", "") else 0,
                hit.get("entry_price", 0),
                hit.get("stop_loss", 0),
                hit.get("target_price", 0),
                verdict.get("grade", ""),
                " | ".join(verdict.get("reasons", [])),
            ),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.debug(f"save_pullback_signal 실패: {e}")
        return False
    finally:
        conn.close()


def get_pullback_signals(days: int = 30) -> list[dict]:
    """최근 N일간 눌림목 시그널 조회 (승률 분석용)."""
    conn = _connect()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT * FROM pullback_signals
           WHERE signal_date >= date('now', ? || ' days', 'localtime')
           ORDER BY signal_date DESC, signal_strength DESC""",
        (f"-{days}",),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_pullback_returns(code: str, signal_date: str,
                            d1: float = None, d2: float = None,
                            d3: float = None, d5: float = None):
    """눌림목 시그널의 D+N 수익률 업데이트 (성과 추적용)."""
    conn = _connect()
    updates = []
    params = []
    if d1 is not None:
        updates.append("d1_return = ?"); params.append(d1)
    if d2 is not None:
        updates.append("d2_return = ?"); params.append(d2)
    if d3 is not None:
        updates.append("d3_return = ?"); params.append(d3)
    if d5 is not None:
        updates.append("d5_return = ?"); params.append(d5)
    if updates:
        params.extend([code, signal_date])
        conn.execute(
            f"UPDATE pullback_signals SET {', '.join(updates)} WHERE code=? AND signal_date=?",
            params,
        )
        conn.commit()
    conn.close()