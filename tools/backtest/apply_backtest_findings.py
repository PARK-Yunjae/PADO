"""
apply_backtest_findings.py — 백테스트 결과를 PADO 코드에 적용
=============================================================
12모듈 170만건 시뮬레이션 결과 기반 패치.

적용 항목:
  1. .env — 점수 가중치 수정 (OBV 3→15, RSI 20→12 등)
  2. .gitignore — tools/backtest/ 대용량 제외
  3. config.py — KONEX 필터 상수 추가
  4. screener.py — OBV 강화 + 코넥스/시장 필터
  5. wave/detector.py — 코넥스 종목 스킵

사용법:
  cd C:\\Coding\\PADO
  python tools\\backtest\\apply_backtest_findings.py
  
  --dry-run  변경 내용만 보여주고 적용 안 함
  --apply    실제 적용 (백업 자동 생성)
"""

import os
import sys
import shutil
import argparse
from pathlib import Path
from datetime import datetime

# PADO 루트 추정
SCRIPT_DIR = Path(__file__).resolve().parent
PADO_ROOT = SCRIPT_DIR.parent.parent  # tools/backtest/ → PADO/


def backup(filepath: Path):
    """파일 백업 (.bak.날짜)"""
    if filepath.exists():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        bak = filepath.with_suffix(f".bak_{ts}")
        shutil.copy2(filepath, bak)
        return bak
    return None


def patch_env(dry_run=False):
    """
    .env 가중치 수정.
    
    백테스트 근거:
    - OBV(모듈J) = 종합1위 D+5 46.2% → 보너스 3→15
    - 거감음봉(I+J) = D+5 50.8% → 보너스 5→12  
    - RSI 15~20 최고 승률 → RSI 가중치 적정 유지하되 과매도 보상 구간 조정 필요
    - 점수 50~60 > 90~100 → 과열감점 강화, RSI 임계값 하향
    - GC 보너스는 교차분석에서 유의미 확인 → 유지
    """
    env_path = PADO_ROOT / ".env"
    if not env_path.exists():
        print(f"  ⚠️ .env 없음: {env_path}")
        return

    with open(env_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 가중치 변경 맵 (현행값 → 새값)
    changes = {
        # OBV: 3→15 (종합 1위 모듈 J, D+J 교집합 최적)
        "CB_SCORE_OBV_BONUS=3": "CB_SCORE_OBV_BONUS=15",
        # 거감음봉: 5→12 (I+J+거감음봉 D+5 50.8%)
        "CB_SCORE_GGE_BONUS=5": "CB_SCORE_GGE_BONUS=12",
        # RSI: 20→12 (RSI 단독 에지는 H모듈에서 확인, CB에선 보조)
        "CB_SCORE_RSI=20": "CB_SCORE_RSI=12",
        # 이평배열: 20→15 (과적합 방지, 상대적 비중 조정)
        "CB_SCORE_MA_ALIGN=20": "CB_SCORE_MA_ALIGN=15",
        # 과열 RSI: 75→70 (점수 90+ 역전 현상 방지)
        "CB_OVERHEAT_RSI=75": "CB_OVERHEAT_RSI=70",
    }

    applied = []
    for old, new in changes.items():
        if old in content:
            content = content.replace(old, new)
            applied.append(f"  {old} → {new}")

    if applied:
        print(f"\n📝 .env 가중치 변경 ({len(applied)}건):")
        for a in applied:
            print(a)

        if not dry_run:
            bak = backup(env_path)
            with open(env_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"  ✅ 적용 완료 (백업: {bak})")
    else:
        print("  .env: 변경할 항목 없음 (이미 적용됐거나 값이 다름)")


def patch_env_example(dry_run=False):
    """.env.example에도 동일 변경 + 백테스트 근거 주석"""
    path = PADO_ROOT / ".env.example"
    if not path.exists():
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # 기존 CB 스코어링 블록을 새 블록으로 교체
    old_block = """# --- ClosingBell 스코어링 ---
CB_SCORE_RSI=20
CB_SCORE_MA_ALIGN=20
CB_SCORE_CHANGE=15
CB_SCORE_VOL_BURST=15
CB_SCORE_BROKER=10
CB_SCORE_SHORT=10
CB_SCORE_GC_BONUS=10
CB_SCORE_GGE_BONUS=5
CB_SCORE_OBV_BONUS=3
CB_OVERHEAT_RSI=75"""

    new_block = """# --- ClosingBell 스코어링 (v4.1 백테스트 기반) ---
# 170만건 시뮬 결과: OBV(J)=종합1위, D+J교집합=46%, 점수50~60>90~100
CB_SCORE_RSI=12
CB_SCORE_MA_ALIGN=15
CB_SCORE_CHANGE=15
CB_SCORE_VOL_BURST=15
CB_SCORE_BROKER=10
CB_SCORE_SHORT=10
CB_SCORE_GC_BONUS=10
CB_SCORE_GGE_BONUS=12
CB_SCORE_OBV_BONUS=15
CB_OVERHEAT_RSI=70"""

    if old_block in content:
        content = content.replace(old_block, new_block)
        if not dry_run:
            backup(path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        print("  ✅ .env.example 업데이트")


def patch_gitignore(dry_run=False):
    """.gitignore에 tools/backtest/ 추가"""
    path = PADO_ROOT / ".gitignore"
    if not path.exists():
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    addition = """
# ─── 백테스트 (대용량 결과) ───
tools/backtest/results/
tools/backtest/universe_whitelist.csv
tools/backtest/universe_whitelist_meta.txt
tools/backtest/*.bak*
"""

    if "tools/backtest/results/" not in content:
        content += addition
        if not dry_run:
            backup(path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        print("  ✅ .gitignore: tools/backtest/ 제외 추가")
    else:
        print("  .gitignore: 이미 추가됨")


def patch_config(dry_run=False):
    """config.py에 코넥스 필터 상수 추가"""
    path = PADO_ROOT / "config.py"
    if not path.exists():
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # CB_ETF_KEYWORDS 다음에 KONEX 필터 추가
    marker = 'CB_ETF_KEYWORDS      = ['
    if 'EXCLUDED_MARKETS' not in content and marker in content:
        insert_after = content.find('\n', content.find(marker))
        if insert_after > 0:
            addition = """

# 코넥스/스팩 제외 (2,661종목→2,547보통주+114우선주, 백테스트 검증)
EXCLUDED_MARKETS     = {"KONEX"}
"""
            content = content[:insert_after+1] + addition + content[insert_after+1:]

            if not dry_run:
                backup(path)
                with open(path, "w", encoding="utf-8") as f:
                    f.write(content)
            print("  ✅ config.py: EXCLUDED_MARKETS 추가")
    else:
        print("  config.py: 이미 추가됐거나 마커 없음")


def patch_wave_detector(dry_run=False):
    """wave/detector.py에 코넥스 필터 추가"""
    path = PADO_ROOT / "wave" / "detector.py"
    if not path.exists():
        print(f"  ⚠️ {path} 없음")
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # scan_all에서 코넥스 필터 추가
    old = '        codes = list(self.stock_map.keys())\n        logger.info(f"파동 스캔 시작: {len(codes)}종목")'
    new = '''        codes = list(self.stock_map.keys())

        # 코넥스 제외 (백테스트: 코넥스 종목 유동성 부족, 실매매 불가)
        try:
            from config import EXCLUDED_MARKETS
            codes = [c for c in codes
                     if self.stock_map.get(c) and
                     getattr(self.stock_map[c], 'market', '') not in EXCLUDED_MARKETS]
        except ImportError:
            pass

        logger.info(f"파동 스캔 시작: {len(codes)}종목")'''

    if 'EXCLUDED_MARKETS' not in content and old in content:
        content = content.replace(old, new)
        if not dry_run:
            backup(path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        print("  ✅ wave/detector.py: 코넥스 필터 추가")
    else:
        print("  wave/detector.py: 이미 적용됐거나 코드 구조 다름")


def patch_screener(dry_run=False):
    """screener.py 점수 로직 미세 조정"""
    path = PADO_ROOT / "closingbell" / "screener.py"
    if not path.exists():
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # 1. OBV 체크를 더 관대하게 (기존: 가격↓+OBV↑만, 추가: OBV MA20 돌파)
    old_obv = '''    def _check_obv(self, df):
        import numpy as np
        if len(df) < 20: return False
        r = df.tail(20)
        ps = np.polyfit(range(20), r["close"].values, 1)[0]
        obv = (np.sign(r["close"].diff()) * r["volume"]).cumsum()
        os_ = np.polyfit(range(len(obv)), obv.values, 1)[0]
        return ps < 0 and os_ > 0'''

    new_obv = '''    def _check_obv(self, df):
        """OBV bull 시그널 (백테스트 모듈J 기반, D+5 46.2%).
        조건 1: 가격↓ + OBV↑ (다이버전스)
        조건 2: OBV가 20일 평균 상향 돌파 (골든크로스)
        """
        import numpy as np
        if len(df) < 21: return False
        r = df.tail(21)
        ps = np.polyfit(range(20), r["close"].iloc[-20:].values, 1)[0]
        obv_vals = (np.sign(r["close"].diff()) * r["volume"]).cumsum()
        os_ = np.polyfit(range(len(obv_vals)), obv_vals.values, 1)[0]

        # 조건 1: 다이버전스 (기존)
        if ps < 0 and os_ > 0:
            return True

        # 조건 2: OBV 골든크로스 (어제 < MA20, 오늘 >= MA20)
        obv_full = (np.sign(df["close"].diff()) * df["volume"]).cumsum()
        obv_ma20 = obv_full.rolling(20).mean()
        if len(obv_full) >= 2 and len(obv_ma20.dropna()) >= 2:
            today_obv = obv_full.iloc[-1]
            today_ma = obv_ma20.iloc[-1]
            yest_obv = obv_full.iloc[-2]
            yest_ma = obv_ma20.iloc[-2]
            if pd.notna(today_ma) and pd.notna(yest_ma):
                if yest_obv < yest_ma and today_obv >= today_ma:
                    return True

        return False'''

    if old_obv in content:
        content = content.replace(old_obv, new_obv)
        if not dry_run:
            backup(path)
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        print("  ✅ screener.py: OBV 체크 강화 (골든크로스 추가)")
    else:
        print("  screener.py: OBV 함수 구조 다름, 수동 확인 필요")


def print_summary():
    """변경 요약"""
    print("\n" + "=" * 60)
    print("📊 백테스트 기반 변경 요약")
    print("=" * 60)
    print("""
┌─────────────────────────────────────────────────┐
│ 변경 항목          현행값    →  새값    근거              │
├─────────────────────────────────────────────────┤
│ CB_SCORE_OBV_BONUS   3     →   15    J모듈 종합1위      │
│ CB_SCORE_GGE_BONUS   5     →   12    I+J+거감음봉 50.8%  │
│ CB_SCORE_RSI        20     →   12    H모듈에서 별도 처리   │
│ CB_SCORE_MA_ALIGN   20     →   15    과적합 방지         │
│ CB_OVERHEAT_RSI     75     →   70    점수역전 방지       │
│ OBV 체크           다이버전스만 → +골든크로스 D+J 교집합 반영   │
│ 파동감지           전종목    → 코넥스 제외  코넥스 에지없음    │
│ .gitignore         -      → backtest/ 제외  526MB 방지   │
└─────────────────────────────────────────────────┘

점수 총점 변화: 기존 ~108점 만점 → ~104점 만점
핵심: OBV가 3점→15점으로 핵심 지표 승격
""")


def main():
    parser = argparse.ArgumentParser(description="백테스트 결과 PADO 적용")
    parser.add_argument("--dry-run", action="store_true", help="변경 내용만 확인")
    parser.add_argument("--apply", action="store_true", help="실제 적용")
    args = parser.parse_args()

    if not args.apply and not args.dry_run:
        print("사용법:")
        print("  python apply_backtest_findings.py --dry-run   # 미리보기")
        print("  python apply_backtest_findings.py --apply     # 적용")
        print_summary()
        return

    dry = args.dry_run
    mode = "DRY RUN (미리보기)" if dry else "적용 모드"

    print(f"\n{'='*60}")
    print(f"🔧 PADO 백테스트 결과 적용 [{mode}]")
    print(f"   PADO 루트: {PADO_ROOT}")
    print(f"{'='*60}")

    print("\n[1/6] .env 가중치...")
    patch_env(dry)

    print("\n[2/6] .env.example...")
    patch_env_example(dry)

    print("\n[3/6] .gitignore...")
    patch_gitignore(dry)

    print("\n[4/6] config.py...")
    patch_config(dry)

    print("\n[5/6] wave/detector.py...")
    patch_wave_detector(dry)

    print("\n[6/6] screener.py...")
    patch_screener(dry)

    print_summary()

    if dry:
        print("⚠️ DRY RUN — 실제 변경 안 됨. --apply로 적용하세요.")
    else:
        print("✅ 모든 패치 적용 완료! 백업 파일은 .bak_날짜 로 저장됨.")


if __name__ == "__main__":
    main()
