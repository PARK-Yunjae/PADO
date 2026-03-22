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

-- v2: 뉴스 매일 축적
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
"""


def init_storage():
    """DB 초기화 — 테이블 + 인덱스 생성."""
    conn = _connect()
    conn.executescript(_TABLES)
    conn.executescript(_INDEXES)
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
                reject_reason,payload,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["scan_date"], row["code"], row.get("name"),
             row["grade"], row.get("confidence"), row.get("action"),
             row.get("chart_state"), row.get("flow_state"),
             row.get("chart_score"), row.get("volume_score"),
             row.get("material_score"), row.get("market_score"),
             row.get("theme_match", 0), row.get("synergy", 0),
             row.get("reject_reason"), _encode(row) if row else None,
             _now()),
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
