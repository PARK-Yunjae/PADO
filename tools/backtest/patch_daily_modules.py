"""
patch_daily_modules.py — master_backtest.py에 B/C/E/K/L 모듈 추가 패치
========================================================================
실행: python tools/backtest/patch_daily_modules.py
→ master_backtest.py의 run_daily_modules 함수를 12모듈 완성본으로 교체

패치 후 전체 실행:
  python tools/backtest/master_backtest.py --all --workers 12
"""

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TARGET = os.path.join(SCRIPT_DIR, "master_backtest.py")

# ── 교체할 새 함수 ──
NEW_FUNCTION = '''
def _get_option_expiry_dates(trading_dates: list) -> set:
    """매월 둘째 목요일(옵션만기일) 근사 계산."""
    expiry = set()
    seen_months = set()
    for d in trading_dates:
        ts = pd.Timestamp(d)
        ym = (ts.year, ts.month)
        if ym in seen_months:
            continue
        first = ts.replace(day=1)
        dow = first.dayofweek
        first_thu = first + pd.Timedelta(days=(3 - dow) % 7)
        second_thu = first_thu + pd.Timedelta(days=7)
        expiry.add(second_thu.strftime('%Y-%m-%d'))
        seen_months.add(ym)
    return expiry


def run_daily_modules(data: dict, active_modules: set, config: dict) -> list:
    """
    모듈 A, B, C, E, K, L: 일자별 전종목 순위가 필요한 모듈 (12모듈 완성본).
    """
    from data_loader import calc_forward_returns
    hold_days = config.get('hold_days', (1,2,3,5,7,10,15,20))
    start_date = config.get('start_date')
    end_date = config.get('end_date')
    signals = []

    all_dates = set()
    for df in data.values():
        all_dates.update(df['date'].dt.strftime('%Y-%m-%d').tolist())
    trading_dates = sorted(all_dates)

    option_expiry = _get_option_expiry_dates(trading_dates)

    # 모듈 E: 기존 D 결과에서 일자별 TOP3 로드
    d_scores = {}
    if 'E' in active_modules:
        mod_d_path = Path(config['results_dir']) / "mod_D_signals.csv"
        if mod_d_path.exists():
            print("  모듈 E: mod_D_signals.csv 로드...")
            d_df = pd.read_csv(mod_d_path, dtype={'code': str}, encoding='utf-8-sig')
            for dt, grp in d_df.groupby('date'):
                top3 = grp.nlargest(3, 'score')
                d_scores[dt] = top3.to_dict('records')
            print(f"  -> {len(d_scores)}거래일 D 점수 로드")
        else:
            print("  !! 모듈 E: mod_D_signals.csv 없음. 먼저 --module D 실행 필요")

    print(f"  일자별 처리: {len(trading_dates)}거래일")

    for idx, date in enumerate(trading_dates):
        if idx < 60 or idx >= len(trading_dates) - 20:
            continue
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue

        daily = []
        for code, df in data.items():
            mask = df['date'] == date
            if mask.sum() == 0:
                continue
            daily.append(df[mask].iloc[0].to_dict())
        if len(daily) < 10:
            continue

        daily_df = pd.DataFrame(daily)
        daily_df = daily_df.sort_values('volume', ascending=False)
        daily_df['vol_rank'] = range(1, len(daily_df) + 1)
        daily_df = daily_df.sort_values('trade_value', ascending=False)
        daily_df['val_rank'] = range(1, len(daily_df) + 1)
        if 'change_rate' in daily_df.columns:
            daily_df = daily_df.sort_values('change_rate', ascending=False)
            daily_df['change_rank'] = range(1, len(daily_df) + 1)

        leading_sectors = []
        if 'sector' in daily_df.columns and 'change_rate' in daily_df.columns:
            sa = daily_df.groupby('sector')['change_rate'].mean()
            if len(sa) > 0:
                leading_sectors = sa.nlargest(3).index.tolist()

        def _fwd(code):
            if code not in data:
                return {}
            df = data[code]
            m = df[df['date'] == date]
            if len(m) == 0:
                return {}
            return calc_forward_returns(df, m.index[0], hold_days)

        def _base(r):
            return {
                'code': r['code'], 'name': r.get('name',''), 'date': date,
                'year': int(date[:4]), 'close': r['close'],
                'stock_type': r.get('stock_type',''),
                'is_preferred': r.get('is_preferred', False),
                'market': r.get('market',''),
            }

        # ── A: 거래량 TOP5 기본 ──
        if 'A' in active_modules:
            for _, r in daily_df.nsmallest(5, 'vol_rank').iterrows():
                fwd = _fwd(r['code'])
                if fwd:
                    signals.append({**_base(r), **fwd, 'module': 'A',
                        'vol_rank': int(r['vol_rank']), 'volume': r['volume'],
                        'trade_value': r.get('trade_value',0),
                        'change_rate': r.get('change_rate',0)})

        # ── B: 거래량 TOP5 + 필터 비교 ──
        if 'B' in active_modules:
            for _, r in daily_df.nsmallest(5, 'vol_rank').iterrows():
                fwd = _fwd(r['code'])
                if not fwd:
                    continue
                tv = r.get('trade_value', 0)
                cr = r.get('change_rate', 0)
                sect = r.get('sector', '')
                vr = r.get('vol_ratio20', 0)
                signals.append({**_base(r), **fwd, 'module': 'B',
                    'vol_rank': int(r['vol_rank']), 'trade_value': tv,
                    'change_rate': cr, 'sector': sect,
                    'is_leading_sector': int(sect in leading_sectors),
                    'f_tv100': int(tv >= 1e10), 'f_tv500': int(tv >= 5e10),
                    'f_leading': int(sect in leading_sectors),
                    'f_rank23': int(int(r['vol_rank']) in (2,3)),
                    'f_volr3': int(vr >= 3.0),
                    'f_cr20': int(cr >= 20), 'f_cr30': int(cr >= 30)})

        # ── C: 거래량 TOP5 + 보조지표 ──
        if 'C' in active_modules:
            for _, r in daily_df.nsmallest(5, 'vol_rank').iterrows():
                fwd = _fwd(r['code'])
                if not fwd:
                    continue
                signals.append({**_base(r), **fwd, 'module': 'C',
                    'vol_rank': int(r['vol_rank']),
                    'cci': r.get('cci', np.nan), 'rsi': r.get('rsi', np.nan),
                    'disparity20': r.get('disparity20', np.nan),
                    'ma20_slope': r.get('ma20_slope', np.nan),
                    'vol_ratio20': r.get('vol_ratio20', np.nan),
                    'consec_bullish': r.get('consec_bullish', 0),
                    'candle_body_pct': r.get('candle_body_pct', np.nan),
                    'is_bullish': r.get('is_bullish', 0),
                    'bb_pctb': r.get('bb_pctb', np.nan),
                    'change_rate': r.get('change_rate', 0),
                    'trade_value': r.get('trade_value', 0)})

        # ── E: 점수제 TOP3 순위별 보유기간 ──
        if 'E' in active_modules and date in d_scores:
            for ri, rec in enumerate(d_scores[date], 1):
                code = str(rec.get('code','')).zfill(6)
                fwd = _fwd(code)
                if fwd:
                    signals.append({'module': 'E', 'code': code,
                        'name': rec.get('name',''), 'date': date,
                        'year': int(date[:4]), 'close': rec.get('close',0),
                        'stock_type': rec.get('stock_type',''),
                        'is_preferred': rec.get('is_preferred', False),
                        'score': rec.get('score', 0), 'score_rank': ri, **fwd})

        # ── K: 시황/캘린더 ──
        if 'K' in active_modules:
            is_exp = date in option_expiry
            dow = pd.Timestamp(date).dayofweek
            mc = daily_df['change_rate'].median() if 'change_rate' in daily_df.columns else 0
            mv = daily_df['vol_ratio20'].median() if 'vol_ratio20' in daily_df.columns else 1
            top1 = daily_df.nsmallest(1, 'vol_rank')
            if len(top1) > 0:
                r = top1.iloc[0]
                fwd = _fwd(r['code'])
                if fwd:
                    signals.append({**_base(r), **fwd, 'module': 'K',
                        'is_option_expiry': int(is_exp),
                        'is_monday': int(dow==0), 'is_friday': int(dow==4),
                        'is_crash_day': int(mc < -2.0),
                        'is_rally_day': int(mc > 2.0),
                        'mkt_change': round(mc, 2), 'mkt_vol': round(mv, 2)})

        # ── L: 150억봉 추격 ──
        if 'L' in active_modules:
            big = daily_df[daily_df['trade_value'] >= 1.5e10]
            if 'is_bullish' in daily_df.columns:
                big = big[big['is_bullish'] == 1]
            for _, r in big.iterrows():
                fwd = _fwd(r['code'])
                if fwd:
                    signals.append({**_base(r), **fwd, 'module': 'L',
                        'trade_value': r.get('trade_value',0),
                        'trade_value_b': round(r.get('trade_value',0)/1e8, 1),
                        'change_rate': r.get('change_rate',0),
                        'vol_ratio20': r.get('vol_ratio20',0),
                        'vol_rank': int(r.get('vol_rank',0))})

        if idx % 250 == 0:
            print(f"    ... {date} ({idx}/{len(trading_dates)})")

    return signals
'''

# ── 패치 실행 ──
def patch():
    if not os.path.exists(TARGET):
        print(f"!! {TARGET} 없음")
        return

    with open(TARGET, 'r', encoding='utf-8') as f:
        content = f.read()

    # 백업
    backup = TARGET + '.bak'
    with open(backup, 'w', encoding='utf-8') as f:
        f.write(content)
    print(f"백업: {backup}")

    # 기존 함수 찾기
    start_marker = "def run_daily_modules(data: dict, active_modules: set, config: dict) -> list:"
    end_marker = "\n\n# ══════════════════════════════════════════════\n# 메인 오케스트레이터"

    si = content.find(start_marker)
    ei = content.find(end_marker, si)

    if si == -1:
        print("!! run_daily_modules 함수를 찾을 수 없음")
        return
    if ei == -1:
        print("!! 종료 마커를 찾을 수 없음")
        return

    new_content = content[:si] + NEW_FUNCTION.strip() + "\n" + content[ei:]

    with open(TARGET, 'w', encoding='utf-8') as f:
        f.write(new_content)

    print(f"패치 완료: {TARGET}")
    print(f"추가된 모듈: B(필터비교), C(보조지표), E(순위별보유), K(캘린더), L(150억봉)")


if __name__ == "__main__":
    patch()
