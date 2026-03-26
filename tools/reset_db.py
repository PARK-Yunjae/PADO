"""
PADO v5 DB 초기화/리셋
=======================
기존 pado.db를 백업하고 v5 스키마로 새로 생성합니다.

실행:
    python tools/reset_db.py              # 백업 후 초기화
    python tools/reset_db.py --no-backup  # 백업 없이 초기화
    python tools/reset_db.py --migrate    # 기존 DB 유지 + v5 테이블만 추가
"""

import argparse
import shutil
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main():
    parser = argparse.ArgumentParser(description="PADO v5 DB 초기화")
    parser.add_argument("--no-backup", action="store_true", help="백업 없이 초기화")
    parser.add_argument("--migrate", action="store_true", help="기존 DB 유지 + v5 테이블 추가만")
    args = parser.parse_args()

    from config import APP_DB_PATH

    db_path = Path(APP_DB_PATH)

    if args.migrate:
        print(f"마이그레이션 모드: {db_path}")
        _migrate(db_path)
        return

    # 백업
    if db_path.exists() and not args.no_backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = db_path.with_name(f"pado_backup_{ts}.db")
        shutil.copy2(db_path, backup)
        print(f"백업 완료: {backup}")
        print(f"  크기: {backup.stat().st_size / 1024:.0f} KB")

    # 삭제
    if db_path.exists():
        db_path.unlink()
        print(f"기존 DB 삭제: {db_path}")

    # 새로 생성
    from shared.storage import init_storage
    init_storage()
    print(f"v5 DB 생성 완료: {db_path}")

    # 테이블 확인
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
    conn.close()

    print(f"\n생성된 테이블 ({len(tables)}개):")
    for t in tables:
        print(f"  ✅ {t}")

    print(f"\n{'='*50}")
    print(f"  PADO v5 DB 초기화 완료")
    print(f"  이제 python main.py --test-all 로 테스트하세요")
    print(f"{'='*50}")


def _migrate(db_path):
    """기존 DB에 v5 테이블만 추가."""
    import sqlite3

    if not db_path.exists():
        print(f"DB 없음 — 전체 초기화로 전환")
        from shared.storage import init_storage
        init_storage()
        return

    conn = sqlite3.connect(str(db_path))

    # v5: pullback_signals 테이블
    conn.execute("""
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
    )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS idx_pullback_date ON pullback_signals(signal_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pullback_code ON pullback_signals(code)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pullback_verdict ON pullback_signals(verdict)")

    # signal_type 컬럼 (v4.1)
    try:
        conn.execute("SELECT signal_type FROM jcgs_scan_results LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE jcgs_scan_results ADD COLUMN signal_type TEXT")
        conn.execute("ALTER TABLE jcgs_scan_results ADD COLUMN recommended_hold_days TEXT")
        conn.execute("ALTER TABLE jcgs_scan_results ADD COLUMN strategy_bucket TEXT")

    conn.commit()
    conn.close()

    print(f"마이그레이션 완료: pullback_signals 테이블 추가")


if __name__ == "__main__":
    main()
