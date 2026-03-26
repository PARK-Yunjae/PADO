"""
과거 N일 소급 시뮬레이션
=========================
최근 N영업일의 눌림목 시그널을 소급 생성하여
pullback_signals 테이블에 저장합니다.

OHLCV 데이터 기반으로 각 날짜 시점에서:
  1차: 거래량 폭발 종목 감시 등록
  2차: 눌림목 감지 (잔존률 기준)
  3차: DART + 공매도 + 거래원 + 뉴스 (현재 시점 기준)
  수익률: D+1~D+5 실제 수익률 계산

실행:
    python tools/backfill.py              # 최근 5영업일
    python tools/backfill.py --days 10    # 최근 10영업일
    python tools/backfill.py --days 5 --verify  # 3차 검증 포함 (API 사용)
"""

import argparse
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import OHLCV_DIR, setup_logging, KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL
from shared import storage

logger = setup_logging().getChild("backfill")

EXPLOSION_MULTI = 3.0
MAX_WATCH_DAYS = 5
D2_THRESHOLD = 0.35
D3_THRESHOLD = 0.20
GENERIC_THRESHOLD = 0.30
BEST_THRESHOLD = 0.12
MIN_PRICE = 2000
MAX_PRICE = 150000


def get_trading_days(n: int) -> list[str]:
    """최근 N영업일 날짜 리스트 (OHLCV 기반)."""
    # 삼성전자로 영업일 추출
    p = OHLCV_DIR / "005930.csv"
    if not p.exists():
        return []
    df = pd.read_csv(p, encoding="utf-8-sig")
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").dropna(subset=["date"])
    dates = df["date"].tail(n + MAX_WATCH_DAYS + 5).tolist()
    return [d.strftime("%Y-%m-%d") for d in dates]


def load_ohlcv(code: str) -> pd.DataFrame | None:
    p = OHLCV_DIR / f"{code}.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig", on_bad_lines="skip")
        df.columns = [c.strip().lower() for c in df.columns]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        for c in ("open", "high", "low", "close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0).astype(int)
        return df.dropna(subset=["close"])
    except Exception:
        return None


def scan_date(df: pd.DataFrame, target_date: str) -> list[dict]:
    """특정 날짜 시점에서 눌림목 시그널 추출."""
    target_dt = pd.to_datetime(target_date)

    # target_date까지의 데이터만 사용 (미래 정보 차단)
    mask = df["date"] <= target_dt
    if mask.sum() < 30:
        return []

    df_cut = df[mask].reset_index(drop=True)
    last_idx = len(df_cut) - 1
    last = df_cut.iloc[last_idx]

    # 실제 날짜가 target_date인지 확인
    if last["date"].strftime("%Y-%m-%d") != target_date:
        return []

    price = float(last["close"])
    if price < MIN_PRICE or price > MAX_PRICE:
        return []

    ma20 = df_cut["volume"].rolling(20).mean()
    ma5 = df_cut["close"].rolling(5).mean()
    ma8 = df_cut["close"].rolling(8).mean()

    signals = []

    # 최근 MAX_WATCH_DAYS 이내 폭발일 탐색
    search_start = max(0, last_idx - MAX_WATCH_DAYS)
    for i in range(last_idx, search_start, -1):
        if pd.isna(ma20.iloc[i]) or ma20.iloc[i] <= 0:
            continue
        ratio = df_cut.iloc[i]["volume"] / ma20.iloc[i]
        if ratio < EXPLOSION_MULTI:
            continue

        d_plus = last_idx - i
        if d_plus < 1 or d_plus > MAX_WATCH_DAYS:
            continue

        exp_vol = int(df_cut.iloc[i]["volume"])
        vol_remain = int(last["volume"]) / exp_vol * 100 if exp_vol > 0 else 100

        # 시그널 판정
        sig_type = ""
        strength = 0

        if d_plus == 2 and vol_remain <= D2_THRESHOLD * 100:
            sig_type = f"D+2 잔존{vol_remain:.0f}%"
            strength = 2
        elif d_plus == 3 and vol_remain <= D3_THRESHOLD * 100:
            sig_type = f"D+3 잔존{vol_remain:.0f}%"
            strength = 3
        elif vol_remain <= BEST_THRESHOLD * 100:
            sig_type = f"극강 잔존{vol_remain:.0f}%"
            strength = 4
        elif vol_remain <= GENERIC_THRESHOLD * 100:
            sig_type = f"일반 잔존{vol_remain:.0f}%"
            strength = 1
        else:
            continue

        is_bearish = last["close"] < last["open"]
        if is_bearish:
            strength += 1

        m5 = ma5.iloc[last_idx]
        m8 = ma8.iloc[last_idx]
        ma_touch = ""
        if m5 > 0 and abs(price - m5) / m5 < 0.02:
            ma_touch = "5일선"
            strength += 1
        elif m8 > 0 and abs(price - m8) / m8 < 0.02:
            ma_touch = "8일선"
            strength += 1

        support = m8 if ma_touch == "8일선" else (m5 if ma_touch == "5일선" else min(m5, m8))
        entry = round(support * 1.005, 0)
        stop = round(support * 0.97, 0)
        high_20d = df_cut["high"].tail(20).max()
        exp_close = float(df_cut.iloc[i]["close"])
        target_p = round(min(exp_close, high_20d) * 0.98, 0)
        if target_p <= entry:
            target_p = round(price * 1.05, 0)

        exp_date = df_cut.iloc[i]["date"].strftime("%m/%d")

        # D+1~D+5 실제 수익률 (전체 데이터에서)
        full_idx = df[df["date"] == target_dt].index
        if len(full_idx) == 0:
            continue
        fi = full_idx[0]

        # 다음날 시가 매수
        if fi + 1 >= len(df):
            continue
        entry_price = float(df.iloc[fi + 1]["open"])
        if entry_price <= 0:
            continue

        returns = {}
        for hold in [1, 2, 3, 5]:
            ex = fi + 1 + hold
            if ex < len(df):
                exit_p = float(df.iloc[ex]["close"])
                returns[f"d{hold}_return"] = round((exit_p - entry_price) / entry_price * 100, 2)

        signals.append({
            "signal_date": target_date,
            "d_plus": d_plus,
            "explosion_date": exp_date,
            "explosion_ratio": round(ratio, 1),
            "vol_ratio_pct": round(vol_remain, 1),
            "signal_strength": strength,
            "ma_touch": ma_touch,
            "is_bearish": is_bearish,
            "entry_price": entry,
            "stop_loss": stop,
            "target_price": target_p,
            "note": f"폭발({ratio:.1f}배) D+{d_plus} 잔존{vol_remain:.0f}%"
                    + (f" {ma_touch}터치" if ma_touch else "")
                    + (" 음봉" if is_bearish else ""),
            **returns,
        })

        break  # 가장 최근 폭발일 1개만

    return signals


def verify_stock(code: str, name: str, api) -> dict:
    """3차 검증 (현재 시점 기준 — 과거 소급 불가)."""
    reasons = []
    warnings = 0
    rejects = 0

    # DART
    try:
        from checkers.dart_checker import check_dart
        dart = check_dart(code)
        grade = dart.get("grade", 4)
        if grade <= 1:
            rejects += 1; reasons.append("❌ DART 극위험")
        elif grade <= 2:
            warnings += 1; reasons.append("⚠️ DART 위험")
        elif grade >= 5:
            reasons.append("✅ DART 호재")
        else:
            reasons.append("DART 중립")
    except:
        reasons.append("DART 조회 실패")

    # 공매도
    try:
        if api:
            time.sleep(0.3)
            shorts = api.get_short_selling(code, days=5)
            if shorts:
                avg = sum(s.get("short_ratio", 0) for s in shorts) / len(shorts)
                if avg >= 10:
                    rejects += 1; reasons.append(f"❌ 공매도 {avg:.1f}%")
                elif avg >= 5:
                    warnings += 1; reasons.append(f"⚠️ 공매도 {avg:.1f}%")
                else:
                    reasons.append(f"✅ 공매도 {avg:.1f}%")
    except:
        reasons.append("공매도 실패")

    # 거래원
    try:
        if api:
            time.sleep(0.3)
            trends = api.get_investor_trend(code, days=5)
            if trends:
                foreign = sum(t.get("foreign", 0) for t in trends)
                inst = sum(t.get("institution", 0) for t in trends)
                if foreign > 0 and inst > 0:
                    reasons.append("✅ 외인+기관 순매수")
                elif foreign > 0:
                    reasons.append(f"외인 순매수")
                elif foreign < 0 and inst < 0:
                    warnings += 1; reasons.append("⚠️ 동반 순매도")
                else:
                    reasons.append("수급 혼조")
    except:
        reasons.append("수급 실패")

    if rejects > 0:
        return {"grade": "REJECT", "reasons": reasons}
    elif warnings >= 2:
        return {"grade": "WARN", "reasons": reasons}
    else:
        return {"grade": "PASS", "reasons": reasons}


def main():
    parser = argparse.ArgumentParser(description="과거 N일 소급 시뮬")
    parser.add_argument("--days", type=int, default=5, help="소급할 영업일 수 (기본 5)")
    parser.add_argument("--verify", action="store_true", help="3차 검증 포함 (키움 API 사용)")
    args = parser.parse_args()

    # 종목 매핑 — stock_mapping.csv 직접 읽기 (모듈 폴백)
    name_map = {}   # code → name
    sector_map = {} # code → sector
    try:
        from shared.stock_map import load_stock_map
        smap = load_stock_map()
        for k, v in smap.items():
            name_map[k] = v.name if hasattr(v, "name") else str(v)
            sector_map[k] = getattr(v, "sector", "기타")
    except Exception:
        pass

    # 폴백: stock_mapping.csv 직접
    if not name_map:
        try:
            import pandas as _pd2
            from config import DATA_DIR
            mp = _pd2.read_csv(str(DATA_DIR / "stock_mapping.csv"), encoding="utf-8-sig", dtype=str)
            mp.columns = [c.strip().lower() for c in mp.columns]
            for _, row in mp.iterrows():
                code = str(row.get("code", "")).zfill(6)
                name_map[code] = str(row.get("name", code))
                sector_map[code] = str(row.get("sector", "기타"))
            print(f"stock_mapping.csv 직접 로드: {len(name_map)}종목")
        except Exception as e:
            print(f"⚠️ 종목 매핑 로드 실패: {e}")

    # 영업일 목록
    all_dates = get_trading_days(args.days + 10)
    target_dates = all_dates[-(args.days):]
    print(f"소급 대상: {target_dates[0]} ~ {target_dates[-1]} ({len(target_dates)}영업일)")

    # 종목 리스트
    codes = sorted([p.stem for p in OHLCV_DIR.glob("*.csv")])

    # 제외 키워드 — 더 강력하게
    EXCLUDE_KW = [
        "ETF", "ETN", "KODEX", "TIGER", "KBSTAR", "ARIRANG", "HANARO", "SOL",
        "스팩", "SPAC", "리츠",
        "호스팩",                     # "키움제10호스팩" 패턴
        "기업인수목적",                # 스팩 정식 명칭
        "인버스", "레버리지",           # ETN 파생
    ]

    def _is_excluded(name: str) -> bool:
        """ETF/스팩/우선주 제외."""
        for kw in EXCLUDE_KW:
            if kw in name:
                return True
        # 우선주
        if name.endswith(("우", "우B", "우C", "우(전환)", "1우", "2우", "3우")):
            return True
        return False

    # 사전 필터 — 제외 종목 미리 빼기
    excluded_codes = set()
    for code in codes:
        name = name_map.get(code, "")
        if name and _is_excluded(name):
            excluded_codes.add(code)
    codes_filtered = [c for c in codes if c not in excluded_codes]
    print(f"스캔 대상: {len(codes_filtered)}종목 (제외 {len(excluded_codes)}개: ETF/스팩/우선주)")

    # API (3차 검증용)
    api = None
    if args.verify and KIWOOM_APPKEY:
        from shared.kiwoom_api import KiwoomAPI
        api = KiwoomAPI(KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL)
        print("키움 API 연결됨 (3차 검증 활성)")
    elif args.verify:
        print("⚠️ 키움 API 키 없음 — 3차 검증 스킵")

    total_saved = 0

    for date_str in target_dates:
        print(f"\n{'─'*50}")
        print(f"📅 {date_str} 소급 중...")

        day_signals = []

        for code in codes_filtered:
            try:
                df = load_ohlcv(code)
            except Exception:
                continue
            if df is None or len(df) < 50:
                continue

            name = name_map.get(code, code)

            # 이중 체크 (이름이 늦게 로드된 경우 대비)
            if _is_excluded(name):
                continue

            signals = scan_date(df, date_str)
            if not signals:
                continue

            for s in signals:
                s["code"] = code
                s["name"] = name
                day_signals.append(s)

        print(f"  눌림목 시그널: {len(day_signals)}건")

        if not day_signals:
            continue

        # 시그널 강도 상위 정렬
        day_signals.sort(key=lambda x: -x["signal_strength"])

        # 3차 검증 (상위 10개만 — API 한도)
        for s in day_signals[:10]:
            if api:
                verdict = verify_stock(s["code"], s["name"], api)
            else:
                verdict = {"grade": "PASS", "reasons": ["검증 스킵 (API 없음)"]}

            s["verdict"] = verdict["grade"]
            s["verify_reasons"] = verdict["reasons"]

            # DB 저장
            hit = {
                "code": s["code"], "name": s["name"],
                "d_plus": s["d_plus"],
                "explosion_date": s["explosion_date"],
                "explosion_ratio": s["explosion_ratio"],
                "vol_ratio_pct": s["vol_ratio_pct"],
                "signal_strength": s["signal_strength"],
                "ma_touch": s["ma_touch"],
                "note": s.get("note", ""),
                "entry_price": s["entry_price"],
                "stop_loss": s["stop_loss"],
                "target_price": s["target_price"],
            }

            saved = storage.save_pullback_signal(date_str, hit, verdict)

            # 수익률도 업데이트
            storage.update_pullback_returns(
                s["code"], date_str,
                d1=s.get("d1_return"),
                d2=s.get("d2_return"),
                d3=s.get("d3_return"),
                d5=s.get("d5_return"),
            )

            if saved:
                total_saved += 1

            icon = {"PASS": "📌", "WARN": "👀", "REJECT": "❌"}.get(verdict["grade"], "?")
            d3 = s.get("d3_return", "")
            d3_str = f"D+3 {d3:+.1f}%" if d3 else ""
            print(f"  {icon} {s['name']:<12} D+{s['d_plus']} "
                  f"잔존{s['vol_ratio_pct']:.0f}% 강도{s['signal_strength']} "
                  f"{d3_str} {verdict['grade']}")

    print(f"\n{'='*50}")
    print(f"  소급 완료: {total_saved}건 저장")
    print(f"{'='*50}")

    # 간단 리포트
    signals = storage.get_pullback_signals(days=args.days + 5)
    if signals:
        df_sig = pd.DataFrame(signals)
        print(f"\n📊 소급 데이터 요약")
        for verdict in ["PASS", "WARN", "REJECT"]:
            sub = df_sig[df_sig["verdict"] == verdict]
            d3 = pd.to_numeric(sub["d3_return"], errors="coerce").dropna()
            if len(d3) > 0:
                wr = (d3 > 0).sum() / len(d3) * 100
                print(f"  {verdict}: {len(sub)}건 D+3 승률 {wr:.1f}% 평균 {d3.mean():+.2f}%")
            else:
                print(f"  {verdict}: {len(sub)}건 (수익률 미집계)")


if __name__ == "__main__":
    main()
