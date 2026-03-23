"""
report_compact.py — 백테스트 결과를 업로드 가능한 크기로 압축 요약
=================================================================
기존 all_signals.csv (388MB)를 분석에 필요한 핵심만 추출.

사용법:
  python report_compact.py                          # 기본
  python report_compact.py --results C:/path/to/results
  python report_compact.py --results C:/path/to/results --all-signals  # 전체 파일 기반

출력 (모두 31MB 미만):
  compact_module_summary.csv       ← 모듈×D+N 승률/수익률 매트릭스
  compact_yearly.csv               ← 모듈×연도×D+N
  compact_preferred.csv            ← 모듈×보통주/우선주×D+N
  compact_score_bands.csv          ← 모듈D 점수구간별 성과
  compact_cross_module.csv         ← 모듈 교차 빈도 (같은 날 같은 종목)
  compact_monthly.csv              ← 월별 추이
  compact_top_signals.csv          ← 모듈별 고승률 조건 탐색
  compact_sector.csv               ← 섹터별 성과
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent
HOLD_DAYS = (1, 2, 3, 5, 7, 10, 15, 20)


def load_signals(results_dir: Path, module: str = None) -> pd.DataFrame:
    """모듈별 또는 전체 시그널 로드"""
    if module:
        path = results_dir / f"mod_{module}_signals.csv"
        if path.exists():
            return pd.read_csv(path, encoding='utf-8-sig')
    else:
        path = results_dir / "all_signals.csv"
        if path.exists():
            return pd.read_csv(path, encoding='utf-8-sig')
    return pd.DataFrame()


def wr(series):
    """승률 계산"""
    valid = series.dropna()
    return round(valid.mean() * 100, 2) if len(valid) > 0 else np.nan


def avg(series):
    """평균 수익률"""
    valid = series.dropna()
    return round(valid.mean(), 4) if len(valid) > 0 else np.nan


def med(series):
    """중앙값"""
    valid = series.dropna()
    return round(valid.median(), 4) if len(valid) > 0 else np.nan


def make_summary_row(df, label, hold_days=HOLD_DAYS):
    """한 그룹의 D+N 요약 행 생성"""
    row = {'group': label, 'n': len(df)}
    for d in hold_days:
        rc = f'D+{d}_return'
        wc = f'D+{d}_win'
        if rc in df.columns:
            row[f'd{d}_avg'] = avg(df[rc])
            row[f'd{d}_med'] = med(df[rc])
        if wc in df.columns:
            row[f'd{d}_wr'] = wr(df[wc])
    return row


def generate_compact_reports(results_dir: Path):
    """전체 압축 리포트 생성"""

    out_dir = results_dir / "compact"
    out_dir.mkdir(exist_ok=True)

    modules = ['D', 'F', 'G', 'H', 'I', 'J']  # 현재 구현된 모듈
    all_data = {}

    print("=" * 60)
    print("📊 압축 리포트 생성기")
    print("=" * 60)

    # ── 모듈별 로드 ──
    for m in modules:
        df = load_signals(results_dir, m)
        if len(df) > 0:
            all_data[m] = df
            print(f"  {m}: {len(df):,}건 로드")

    if not all_data:
        print("❌ 시그널 데이터 없음")
        return

    # ══════════════════════════════════════
    # 1. 모듈별 요약 (가장 핵심)
    # ══════════════════════════════════════
    print("\n[1/7] 모듈별 요약...")
    rows = []
    for m, df in all_data.items():
        rows.append(make_summary_row(df, m))
        # 보통주만
        common = df[df.get('is_preferred', pd.Series([False]*len(df))) == False] if 'is_preferred' in df.columns else df
        rows.append(make_summary_row(common, f"{m}_common"))
        # 우선주만
        if 'is_preferred' in df.columns:
            pref = df[df['is_preferred'] == True]
            if len(pref) > 0:
                rows.append(make_summary_row(pref, f"{m}_preferred"))

    pd.DataFrame(rows).to_csv(out_dir / "module_summary.csv", index=False)

    # ══════════════════════════════════════
    # 2. 연도별 × 모듈별
    # ══════════════════════════════════════
    print("[2/7] 연도별...")
    rows = []
    for m, df in all_data.items():
        if 'year' not in df.columns and 'date' in df.columns:
            df['year'] = pd.to_datetime(df['date']).dt.year
        if 'year' in df.columns:
            for year, ydf in df.groupby('year'):
                row = make_summary_row(ydf, f"{m}_{year}")
                row['module'] = m
                row['year'] = year
                rows.append(row)

    pd.DataFrame(rows).to_csv(out_dir / "yearly.csv", index=False)

    # ══════════════════════════════════════
    # 3. 월별 추이 (레짐 변화 추적)
    # ══════════════════════════════════════
    print("[3/7] 월별...")
    rows = []
    for m, df in all_data.items():
        if 'date' in df.columns:
            df = df.copy()
            df['ym'] = pd.to_datetime(df['date']).dt.to_period('M').astype(str)
            for ym, mdf in df.groupby('ym'):
                row = make_summary_row(mdf, f"{m}_{ym}")
                row['module'] = m
                row['ym'] = ym
                rows.append(row)

    pd.DataFrame(rows).to_csv(out_dir / "monthly.csv", index=False)

    # ══════════════════════════════════════
    # 4. 점수 구간별 (모듈 D)
    # ══════════════════════════════════════
    print("[4/7] 점수 구간별 (모듈 D)...")
    if 'D' in all_data and 'score' in all_data['D'].columns:
        df_d = all_data['D'].copy()
        bins = [0, 30, 40, 50, 60, 70, 80, 90, 100]
        labels = ['0-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80-90', '90-100']
        df_d['score_band'] = pd.cut(df_d['score'], bins=bins, labels=labels, right=False)

        rows = []
        for band, bdf in df_d.groupby('score_band', observed=True):
            row = make_summary_row(bdf, str(band))
            row['score_band'] = str(band)
            rows.append(row)

        pd.DataFrame(rows).to_csv(out_dir / "score_bands.csv", index=False)

    # ══════════════════════════════════════
    # 5. 섹터별 성과
    # ══════════════════════════════════════
    print("[5/7] 섹터별...")
    # 화이트리스트에서 섹터 매핑
    wl_path = SCRIPT_DIR / "universe_whitelist.csv"
    sector_map = {}
    if wl_path.exists():
        wl = pd.read_csv(wl_path, dtype={'code': str}, encoding='utf-8-sig')
        sector_map = dict(zip(wl['code'].str.zfill(6), wl.get('sector', pd.Series())))

    rows = []
    for m, df in all_data.items():
        df = df.copy()
        if 'code' in df.columns and sector_map:
            df['sector'] = df['code'].astype(str).str.zfill(6).map(sector_map)
            top_sectors = df['sector'].value_counts().head(30).index
            for sect, sdf in df[df['sector'].isin(top_sectors)].groupby('sector'):
                row = make_summary_row(sdf, f"{m}_{sect}")
                row['module'] = m
                row['sector'] = sect
                rows.append(row)

    if rows:
        pd.DataFrame(rows).to_csv(out_dir / "sector.csv", index=False)

    # ══════════════════════════════════════
    # 6. 교차 분석 (같은 날짜+종목에 여러 모듈 시그널)
    # ══════════════════════════════════════
    print("[6/7] 교차 분석...")
    # 날짜+종목별 어떤 모듈들이 동시에 시그널을 냈는지
    all_sigs = []
    for m, df in all_data.items():
        if 'date' in df.columns and 'code' in df.columns:
            sub = df[['date', 'code']].copy()
            sub['module'] = m
            all_sigs.append(sub)

    if all_sigs:
        merged = pd.concat(all_sigs)
        cross = merged.groupby(['date', 'code'])['module'].apply(lambda x: '+'.join(sorted(x.unique()))).reset_index()
        cross.columns = ['date', 'code', 'modules']
        cross_counts = cross['modules'].value_counts().head(50)

        # 교차 조합별 성과
        rows = []
        for combo, cnt in cross_counts.items():
            if '+' not in combo:
                continue  # 단일 모듈은 스킵
            combo_pairs = cross[cross['modules'] == combo][['date', 'code']]
            # 첫 번째 모듈의 수익률 사용
            first_mod = combo.split('+')[0]
            if first_mod in all_data:
                mod_df = all_data[first_mod]
                merged_perf = combo_pairs.merge(mod_df, on=['date', 'code'], how='inner')
                if len(merged_perf) > 10:
                    row = make_summary_row(merged_perf, combo)
                    row['combo'] = combo
                    rows.append(row)

        if rows:
            pd.DataFrame(rows).to_csv(out_dir / "cross_module.csv", index=False)

    # ══════════════════════════════════════
    # 7. 모듈별 고승률 조건 탐색
    # ══════════════════════════════════════
    print("[7/7] 고승률 조건 탐색...")
    rows = []

    # 모듈 D: CCI 구간별
    if 'D' in all_data and 'cci_val' in all_data['D'].columns:
        df_d = all_data['D']
        cci_bins = [(-999, 0), (0, 100), (100, 140), (140, 160), (160, 180), (180, 220), (220, 999)]
        for lo, hi in cci_bins:
            sub = df_d[(df_d['cci_val'] >= lo) & (df_d['cci_val'] < hi)]
            if len(sub) > 100:
                row = make_summary_row(sub, f"D_cci_{lo}_{hi}")
                row['condition'] = f'CCI {lo}~{hi}'
                rows.append(row)

    # 모듈 H: RSI 구간별
    if 'H' in all_data and 'rsi_val' in all_data['H'].columns:
        df_h = all_data['H']
        rsi_bins = [(0, 15), (15, 20), (20, 25), (25, 30)]
        for lo, hi in rsi_bins:
            sub = df_h[(df_h['rsi_val'] >= lo) & (df_h['rsi_val'] < hi)]
            if len(sub) > 50:
                row = make_summary_row(sub, f"H_rsi_{lo}_{hi}")
                row['condition'] = f'RSI {lo}~{hi}'
                rows.append(row)

    # 모듈 G: 거래대금 구간별
    if 'G' in all_data and 'trade_value_b' in all_data['G'].columns:
        df_g = all_data['G']
        tv_bins = [(100, 300), (300, 500), (500, 1000), (1000, 9999)]
        for lo, hi in tv_bins:
            sub = df_g[(df_g['trade_value_b'] >= lo) & (df_g['trade_value_b'] < hi)]
            if len(sub) > 100:
                row = make_summary_row(sub, f"G_tv_{lo}_{hi}")
                row['condition'] = f'거래대금 {lo}~{hi}억'
                rows.append(row)

    if rows:
        pd.DataFrame(rows).to_csv(out_dir / "conditions.csv", index=False)

    # ── 파일 크기 확인 ──
    print("\n" + "=" * 60)
    print("✅ 압축 리포트 생성 완료")
    print("=" * 60)
    total_size = 0
    for f in sorted(out_dir.glob("*.csv")):
        size_kb = f.stat().st_size / 1024
        total_size += size_kb
        ok = "✅" if size_kb < 31 * 1024 else "⚠️ 31MB 초과"
        print(f"  {f.name:<30} {size_kb:>8.1f} KB  {ok}")
    print(f"\n  총 크기: {total_size/1024:.1f} MB")
    print(f"  📁 {out_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='백테스트 결과 압축 리포트')
    parser.add_argument('--results', '-r', default=str(SCRIPT_DIR / 'results'),
                        help='results 폴더 경로')
    args = parser.parse_args()

    generate_compact_reports(Path(args.results))
