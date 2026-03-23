"""
deep_cross_analysis.py — 3중 교차 분석 (로컬 실행)
===================================================
모듈 D+J+조건, D+J+섹터, H+I+조건 등 3중 교차 승률 분석.
388MB all_signals.csv를 읽어서 처리.

사용법:
  python deep_cross_analysis.py
  python deep_cross_analysis.py --results C:/Coding/PADO/tools/backtest/results

출력: results/deep_cross_results.csv (업로드 가능 크기)
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from itertools import combinations

SCRIPT_DIR = Path(__file__).parent
HOLD_DAYS = (1, 2, 3, 5, 7, 10, 15, 20)


def wr(s):
    v = s.dropna()
    return round(v.mean() * 100, 2) if len(v) > 0 else np.nan


def avg(s):
    v = s.dropna()
    return round(v.mean(), 4) if len(v) > 0 else np.nan


def row_summary(df, label):
    r = {'label': label, 'n': len(df)}
    for d in HOLD_DAYS:
        wc = f'D+{d}_win'
        rc = f'D+{d}_return'
        if wc in df.columns:
            r[f'd{d}_wr'] = wr(df[wc])
        if rc in df.columns:
            r[f'd{d}_avg'] = avg(df[rc])
    return r


def load_module(results_dir, mod):
    p = results_dir / f"mod_{mod}_signals.csv"
    if p.exists():
        return pd.read_csv(p, dtype={'code': str}, encoding='utf-8-sig')
    return pd.DataFrame()


def main(results_dir):
    results_dir = Path(results_dir)
    rows = []

    print("=" * 70)
    print("🔬 3중 교차 분석")
    print("=" * 70)

    # ── 모듈 로드 ──
    D = load_module(results_dir, 'D')
    J = load_module(results_dir, 'J')
    H = load_module(results_dir, 'H')
    I = load_module(results_dir, 'I')
    G = load_module(results_dir, 'G')
    F = load_module(results_dir, 'F')

    # 화이트리스트에서 섹터 매핑
    wl_path = SCRIPT_DIR / "universe_whitelist.csv"
    sector_map = {}
    if wl_path.exists():
        wl = pd.read_csv(wl_path, dtype={'code': str}, encoding='utf-8-sig')
        sector_map = dict(zip(wl['code'].str.zfill(6), wl.get('sector', pd.Series())))

    # ══════════════════════════════════════
    # 1. D+J 교집합 × 추가 조건
    # ══════════════════════════════════════
    print("\n[1] D+J 교집합 × 추가 조건...")
    if len(D) > 0 and len(J) > 0:
        # date+code 기준 교집합
        dj = D.merge(J[['date', 'code']], on=['date', 'code'], how='inner', suffixes=('', '_j'))
        print(f"  D+J 교집합: {len(dj):,}건")
        rows.append(row_summary(dj, 'D+J'))

        # D+J × CCI 구간
        if 'cci_val' in dj.columns:
            for lo, hi, label in [(0, 100, 'CCI<100'), (100, 140, 'CCI100~140'),
                                   (140, 180, 'CCI140~180'), (180, 999, 'CCI180+')]:
                sub = dj[(dj['cci_val'] >= lo) & (dj['cci_val'] < hi)]
                if len(sub) >= 50:
                    rows.append(row_summary(sub, f'D+J+{label}'))

        # D+J × RSI 구간
        if 'rsi_val' in dj.columns:
            for lo, hi, label in [(0, 30, 'RSI<30'), (30, 50, 'RSI30~50'),
                                   (50, 70, 'RSI50~70'), (70, 100, 'RSI70+')]:
                sub = dj[(dj['rsi_val'] >= lo) & (dj['rsi_val'] < hi)]
                if len(sub) >= 50:
                    rows.append(row_summary(sub, f'D+J+{label}'))

        # D+J × 점수 구간
        if 'score' in dj.columns:
            for lo, hi, label in [(50, 60, 'S50~60'), (60, 70, 'S60~70'),
                                   (70, 80, 'S70~80'), (80, 100, 'S80+')]:
                sub = dj[(dj['score'] >= lo) & (dj['score'] < hi)]
                if len(sub) >= 50:
                    rows.append(row_summary(sub, f'D+J+{label}'))

        # D+J × 섹터
        if sector_map:
            dj['sector'] = dj['code'].astype(str).str.zfill(6).map(sector_map)
            top_sectors = [
                '전동기, 발전기 및 전기 변환 · 공급 · 제어 장치 제조업',
                '금융 지원 서비스업', '기타 금융업', '전자부품 제조업',
                '1차 철강 제조업', '반도체 제조업', '특수 목적용 기계 제조업',
                '통신 및 방송 장비 제조업', '소프트웨어 개발 및 공급업',
            ]
            for sect in top_sectors:
                sub = dj[dj['sector'] == sect]
                if len(sub) >= 30:
                    short = sect[:12]
                    rows.append(row_summary(sub, f'D+J+{short}'))

        # D+J × 거래대금 구간
        if 'trade_value' in dj.columns:
            dj['tv_b'] = dj['trade_value'] / 1e8
            for lo, hi, label in [(0, 100, 'TV<100억'), (100, 300, 'TV100~300억'),
                                   (300, 1000, 'TV300~1000억'), (1000, 99999, 'TV1000억+')]:
                sub = dj[(dj['tv_b'] >= lo) & (dj['tv_b'] < hi)]
                if len(sub) >= 50:
                    rows.append(row_summary(sub, f'D+J+{label}'))

        # D+J × 우선주/보통주
        if 'is_preferred' in dj.columns:
            for pv, label in [(False, '보통주'), (True, '우선주')]:
                sub = dj[dj['is_preferred'] == pv]
                if len(sub) >= 30:
                    rows.append(row_summary(sub, f'D+J+{label}'))

    # ══════════════════════════════════════
    # 2. H(RSI반전) × 추가 조건
    # ══════════════════════════════════════
    print("[2] H(RSI반전) × 추가 조건...")
    if len(H) > 0:
        if 'rsi_val' in H.columns:
            for lo, hi, label in [(0, 15, 'RSI<15'), (15, 20, 'RSI15~20'),
                                   (20, 25, 'RSI20~25'), (25, 30, 'RSI25~30')]:
                sub = H[(H['rsi_val'] >= lo) & (H['rsi_val'] < hi)]
                if len(sub) >= 30:
                    rows.append(row_summary(sub, f'H+{label}'))

        # H × 섹터
        if sector_map:
            H_s = H.copy()
            H_s['sector'] = H_s['code'].astype(str).str.zfill(6).map(sector_map)
            for sect in H_s['sector'].value_counts().head(10).index:
                sub = H_s[H_s['sector'] == sect]
                if len(sub) >= 20:
                    short = sect[:12]
                    rows.append(row_summary(sub, f'H+{short}'))

    # ══════════════════════════════════════
    # 3. I+J (유목민+OBV) × 추가 조건
    # ══════════════════════════════════════
    print("[3] I+J 교집합...")
    if len(I) > 0 and len(J) > 0:
        ij = I.merge(J[['date', 'code']], on=['date', 'code'], how='inner')
        rows.append(row_summary(ij, 'I+J'))
        if len(ij) >= 30:
            # 거감음봉만
            if 'is_bearish' in ij.columns:
                bearish = ij[ij['is_bearish'] == True]
                if len(bearish) >= 10:
                    rows.append(row_summary(bearish, 'I+J+거감음봉'))

    # ══════════════════════════════════════
    # 4. G+H (K값돌파+RSI반전) — 12건이었지만 확장
    # ══════════════════════════════════════
    print("[4] G+H 교집합...")
    if len(G) > 0 and len(H) > 0:
        gh = G.merge(H[['date', 'code']], on=['date', 'code'], how='inner')
        rows.append(row_summary(gh, 'G+H'))

    # ══════════════════════════════════════
    # 5. D+F (점수제+눌림목) × 추가
    # ══════════════════════════════════════
    print("[5] D+F 교집합...")
    if len(D) > 0 and len(F) > 0:
        df_ = D.merge(F[['date', 'code']], on=['date', 'code'], how='inner')
        rows.append(row_summary(df_, 'D+F'))
        if 'score' in df_.columns:
            high = df_[df_['score'] >= 70]
            if len(high) >= 30:
                rows.append(row_summary(high, 'D+F+S70+'))

    # ══════════════════════════════════════
    # 6. D+J+H (3중 교차 — 핵심)
    # ══════════════════════════════════════
    print("[6] D+J+H 3중 교차...")
    if len(D) > 0 and len(J) > 0 and len(H) > 0:
        djh = D.merge(J[['date', 'code']], on=['date', 'code'], how='inner')
        djh = djh.merge(H[['date', 'code']], on=['date', 'code'], how='inner')
        rows.append(row_summary(djh, 'D+J+H'))
        print(f"  D+J+H: {len(djh)}건")

    # ══════════════════════════════════════
    # 7. 연도별 D+J
    # ══════════════════════════════════════
    print("[7] D+J 연도별...")
    if len(D) > 0 and len(J) > 0:
        dj = D.merge(J[['date', 'code']], on=['date', 'code'], how='inner')
        if 'year' in dj.columns:
            for year, ydf in dj.groupby('year'):
                rows.append(row_summary(ydf, f'D+J_{year}'))

    # ── 결과 저장 ──
    result = pd.DataFrame(rows)
    result = result.sort_values('d5_wr', ascending=False)

    out_path = results_dir / "deep_cross_results.csv"
    result.to_csv(out_path, index=False, encoding='utf-8-sig')

    print(f"\n{'='*70}")
    print(f"✅ 3중 교차 분석 완료: {len(rows)}개 조합")
    print(f"📁 {out_path}")
    print(f"{'='*70}")

    # 상위 20개 출력
    print(f"\n🔥 D+5 승률 TOP 20")
    print(f"{'조합':<25} {'건수':>8} {'D+3':>7} {'D+5':>7} {'D+10':>7} {'D+20':>7} {'avg5':>8}")
    print("-" * 75)
    for _, r in result.head(20).iterrows():
        print(f"  {r['label']:<23} {int(r['n']):>8,}  {r.get('d3_wr',0):>5.1f}%  {r.get('d5_wr',0):>5.1f}%  {r.get('d10_wr',0):>5.1f}%  {r.get('d20_wr',0):>5.1f}%  {r.get('d5_avg',0):>+6.2f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--results', default=str(SCRIPT_DIR / 'results'))
    args = parser.parse_args()
    main(args.results)
