"""
PADO v5 — 눌림목 단타 + 재차거시 검증 시스템
================================================
08:25  📰 아침 브리핑 (해외시황 + 외신번역 + 키워드 + AI추론)
12:00  📊 장중 시그널 (코스피/코스닥 + 주도테마 + 국내뉴스)
15:00  🎯 ClosingBell (1차감시→2차눌림목→3차재차거시→매수추천)
15:35  📊 장후 파이프라인 (OHLCV + 글로벌 + DART + 뉴스)

python main.py              → 스케줄러 모드
python main.py --test-all   → 전체 테스트
python main.py --cb-pick    → ClosingBell만
python main.py --briefing   → 아침 브리핑만
python main.py --midday     → 장중 시그널만
"""

import sys
import signal
import argparse
from datetime import datetime, date

from config import (
    KIWOOM_APPKEY, KIWOOM_SECRETKEY, KIWOOM_BASE_URL,
    setup_logging,
)
from shared.kiwoom_api import KiwoomAPI
from shared import storage
from shared.notifier import Notifier

logger = setup_logging()


class App:
    """PADO 통합 앱."""

    def __init__(self):
        logger.info("=" * 50)
        logger.info("PADO 시작")
        logger.info("=" * 50)

        # 싱글톤 초기화
        storage.init_storage()

        self.api = KiwoomAPI(
            appkey=KIWOOM_APPKEY,
            secretkey=KIWOOM_SECRETKEY,
            base_url=KIWOOM_BASE_URL,
        ) if KIWOOM_APPKEY else None

        self.notifier = Notifier()
        self._today = date.today().isoformat()
        self._sent_pullback_codes = set()  # v4.2: 눌림목 중복 발송 방지

    # ─────────────────────────────────────
    # 08:20 아침 뉴스 수집 (v3)
    # ─────────────────────────────────────

    def _collect_morning_news(self):
        """📰 아침 뉴스 수집 + 분석 (브리핑 전에 실행)."""
        logger.info("── 08:20 아침 뉴스 수집 ──")

        try:
            from checkers.news_intelligence import run_news_collection, run_news_analysis
            stats = run_news_collection(self._today)
            logger.info(f"📰 아침 수집: RSS {stats.get('google_rss',0)} + 네이버 {stats.get('naver',0)} = {stats.get('total',0)}건")

            # 델타 감지 + Gemini 분석 (전일 데이터 있을 때만 의미 있음)
            analysis = run_news_analysis(self._today)
            if analysis:
                themes = [t.get("keyword", "") for t in analysis.get("emerging_themes", [])[:3]]
                logger.info(f"📰 분석: 부상 {themes}, 분위기 {analysis.get('market_mood','?')}")
        except Exception as e:
            logger.warning(f"아침 뉴스 수집 실패: {e}")

    # ─────────────────────────────────────
    # 08:30 아침 브리핑
    # ─────────────────────────────────────

    def run_morning_briefing(self):
        """📰 아침 브리핑 — 해외시황 + 외신 + 뉴스 키워드 + 전일 CB A/B."""
        logger.info("── 08:25 아침 브리핑 ──")

        from shared.notifier import embed as _embed, field as _field, COLOR_BLUE, COLOR_GREEN
        embeds_to_send = []

        # ── 1) 해외시황 (나스닥/S&P/VIX) + 외신 TOP5 ──
        try:
            global_fields = []
            from config import GLOBAL_CSV
            import pandas as _pd
            if GLOBAL_CSV.exists():
                gdf = _pd.read_csv(str(GLOBAL_CSV))
                gdf.columns = [c.strip().lower() for c in gdf.columns]
                if len(gdf) >= 2:
                    last, prev = gdf.iloc[-1], gdf.iloc[-2]
                    parts = []
                    for col, label in [("nasdaq", "나스닥"), ("sp500", "S&P 500"), ("vix", "VIX")]:
                        if col in gdf.columns:
                            val, pval = last.get(col, 0), prev.get(col, 0)
                            if pval and val:
                                chg = (val - pval) / pval * 100 if col != "vix" else val - pval
                                if col == "vix":
                                    parts.append(f"VIX **{val:.1f}** ({chg:+.1f})")
                                else:
                                    parts.append(f"{label} **{val:,.0f}** ({chg:+.2f}%)")
                    if parts:
                        global_fields.append(_field("📊 해외 시황", "\n".join(parts)))

            # 외신 TOP5 (한글 번역)
            en_news = storage.get_news_v2_by_date(self._today)
            if not en_news:
                from datetime import timedelta
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                en_news = storage.get_news_v2_by_date(yesterday)
            en_items = [n for n in en_news if n.get("lang") == "en"]
            if en_items:
                seen_cats = set()
                top_en = []
                for item in en_items:
                    cat = item.get("category", "global")
                    if cat not in seen_cats and len(top_en) < 5:
                        seen_cats.add(cat)
                        top_en.append(item.get("title", "")[:100])

                # Gemini 번역 시도
                translated = top_en
                try:
                    from config import GEMINI_API_KEY, GEMINI_MODEL
                    if GEMINI_API_KEY and top_en:
                        import requests as _req, re as _re
                        titles_text = "\n".join(f"{i+1}. {t}" for i, t in enumerate(top_en))
                        prompt = (
                            "영문 뉴스 제목을 한국어로 번역하세요.\n"
                            "규칙: 번호와 번역만 출력. 설명/인사말/프리앰블 절대 금지.\n"
                            "형식: 1. 번역문\n\n"
                            f"{titles_text}"
                        )
                        resp = _req.post(
                            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
                            json={"contents": [{"parts": [{"text": prompt}]}]},
                            timeout=10,
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                            if text:
                                # "1. 번역문" 패턴만 추출
                                numbered = _re.findall(r'\d+\.\s*(.+)', text)
                                if len(numbered) >= len(top_en):
                                    translated = numbered[:len(top_en)]
                except Exception as e:
                    logger.debug(f"외신 번역 실패 (원문 사용): {e}")

                if translated:
                    lines = [f"  • {t}" for t in translated]
                    global_fields.append(_field("🌐 외신 주요 뉴스", "\n".join(lines)))

            if global_fields:
                embeds_to_send.append(_embed("📰 아침 브리핑", "", COLOR_BLUE, global_fields,
                                             footer="Google RSS + 글로벌 지수"))
        except Exception as e:
            logger.warning(f"해외시황 조회 실패: {e}")

        # ── 2) 뉴스 키워드 + Gemini 추론 ──
        try:
            analysis = storage.get_news_analysis(self._today)
            if not analysis:
                from datetime import timedelta
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                analysis = storage.get_news_analysis(yesterday)
            if analysis and analysis.get("emerging"):
                emerging = analysis["emerging"][:5]
                gemini = analysis.get("gemini_result", {})
                kw_lines = []
                for e in emerging:
                    delta_str = e["delta"] if isinstance(e["delta"], str) else f"+{e['delta']}배"
                    kw_lines.append(f"  • **{e['word']}** ({e['today']}건, {delta_str})")
                theme_lines = []
                for t in gemini.get("emerging_themes", [])[:5]:
                    kw, chain = t.get("keyword", ""), t.get("chain", "")
                    stocks = ", ".join(t.get("stocks", [])[:3])
                    if kw:
                        line = f"  • **{kw}**"
                        if chain: line += f" → {chain}"
                        if stocks: line += f" → 📌 {stocks}"
                        theme_lines.append(line)
                mood = gemini.get("market_mood", "")
                news_fields = []
                if kw_lines:
                    news_fields.append(_field("🔮 부상 키워드", "\n".join(kw_lines)))
                if theme_lines:
                    news_fields.append(_field("🧠 AI 추론", "\n".join(theme_lines)))
                if news_fields:
                    embeds_to_send.append(_embed("🔮 뉴스 키워드", f"분위기: **{mood}**" if mood else "",
                                                 COLOR_BLUE, news_fields, footer="델타 감지 + Gemini"))
        except Exception as e:
            logger.debug(f"뉴스 분석 실패: {e}")

        # ── 3) CB 감시 현황 (간단히) ──
        try:
            cb_watching = storage.get_cb_watching()
            if cb_watching:
                names = ", ".join(w.get("name", "?") for w in cb_watching[:8])
                count = len(cb_watching)
                # D+2~D+3 도래 종목 강조
                near = [w.get("name", "?") for w in cb_watching
                        if w.get("days_since", 99) in (1, 2)]
                near_str = f"\n⏰ 내일 D+2~D+3: **{', '.join(near)}**" if near else ""
                from shared.notifier import embed as _e2, field as _f2, COLOR_GREEN
                embeds_to_send.append(_e2(
                    f"👀 CB 감시 ({count}건)", "",
                    COLOR_GREEN,
                    [_f2("감시 종목", names + near_str)],
                    footer="D+5까지 추적"))
        except Exception as e:
            logger.debug(f"CB 감시 현황: {e}")

        # ── 발송 ──
        if embeds_to_send:
            self.notifier.send_pado(embeds_to_send[:10])
            logger.info(f"📰 아침 브리핑 발송: {len(embeds_to_send)}개 embed")
        else:
            logger.info("아침 브리핑: 발송할 내용 없음")

    def _load_market_from_db(self):
        """DB에서 가장 최근 시황 읽기. 없으면 실시간 평가."""
        import json as _json
        from jaechageosi.result_types import MarketResult
        try:
            import sqlite3
            from config import APP_DB_PATH
            conn = sqlite3.connect(str(APP_DB_PATH))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM market_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
            conn.close()
            if row:
                themes = _json.loads(row["leading_themes"]) if row["leading_themes"] else []
                return MarketResult(
                    date=row["date"], score=row["score"] or 0,
                    mode=row["mode"] or "mixed",
                    leading_themes=themes,
                    dangerous=bool(row["dangerous"]),
                    nasdaq_change=row["nasdaq_chg"] or 0.0,
                    kospi_ma20_gap=0.0,
                    seasonal_note="",
                    reasons=[f"DB 저장값 ({row['date']})"],
                )
        except Exception as e:
            logger.debug(f"시황 DB 읽기 실패: {e}")

        # 폴백: 실시간 평가 (캐시 없어도 키움만으로)
        from jaechageosi.market_engine import MarketEngine
        engine = MarketEngine(api=self.api)
        return engine.evaluate(self._today)

    # ─────────────────────────────────────
    # 12:00 장중 시그널 리포트
    # ─────────────────────────────────────

    def run_midday_check(self):
        """📊 장중 시그널 — 코스피/코스닥 + 주도섹터 + 뉴스 리마인드."""
        logger.info("── 12:00 장중 시그널 ──")

        from shared.notifier import embed as _embed, field as _field, COLOR_BLUE
        fields_list = []

        # ── 1) 코스피/코스닥 실시간 (키움 API) ──
        try:
            if self.api:
                import time
                time.sleep(0.3)
                # 코스피 현재가 (001 시장지표)
                kospi = self.api.get_current_price("001")  # 코스피 지수
                time.sleep(0.3)
                kosdaq = self.api.get_current_price("101")  # 코스닥 지수
                parts = []
                if kospi.get("price"):
                    chg = kospi.get("change_rate", 0)
                    parts.append(f"코스피 **{kospi['price']:,}** ({chg:+.2f}%)")
                if kosdaq.get("price"):
                    chg = kosdaq.get("change_rate", 0)
                    parts.append(f"코스닥 **{kosdaq['price']:,}** ({chg:+.2f}%)")
                if parts:
                    fields_list.append(_field("📊 장중 시황", "\n".join(parts)))
        except Exception as e:
            logger.debug(f"지수 조회 실패: {e}")
            # 폴백: DB에서 어제 시황
            try:
                market = self._load_market_from_db()
                if market:
                    mode_kr = {"theme_strong": "강한 테마장", "index_rally": "지수 랠리",
                               "mixed": "혼조", "risk_off": "위험 회피"}.get(market.mode, "")
                    themes = ", ".join(market.leading_themes[:3]) if market.leading_themes else ""
                    fields_list.append(_field("📊 시황 (전일 기준)",
                                             f"{mode_kr} ({market.score}점)\n주도: {themes}"))
            except Exception:
                pass

        # ── 2) 장중 주도섹터 (키움 테마 API) ──
        try:
            if self.api:
                import time
                time.sleep(0.3)
                themes = self.api.get_theme_groups(sort="3", period="1")
                if themes:
                    top3 = themes[:3]
                    lines = []
                    for t in top3:
                        name = t.get("name", "?")
                        chg = t.get("change_rate", 0)
                        lines.append(f"  • **{name}** ({chg:+.1f}%)")
                    if lines:
                        fields_list.append(_field("🔥 장중 주도 테마", "\n".join(lines)))
        except Exception as e:
            logger.debug(f"테마 조회 실패: {e}")

        # ── 3) 국내 뉴스 키워드 리마인드 (한글만) ──
        try:
            analysis = storage.get_news_analysis(self._today)
            if not analysis:
                from datetime import timedelta
                yesterday = (date.today() - timedelta(days=1)).isoformat()
                analysis = storage.get_news_analysis(yesterday)
            if analysis and analysis.get("emerging"):
                import re as _re
                # 한글 포함된 키워드만 필터
                ko_emerging = [e for e in analysis["emerging"]
                               if _re.search(r'[가-힣]', e.get("word", ""))]
                top3 = ko_emerging[:5]
                lines = []
                for e in top3:
                    delta_str = e["delta"] if isinstance(e["delta"], str) else f"+{e['delta']}배"
                    lines.append(f"  • **{e['word']}** ({e['today']}건, {delta_str})")
                if lines:
                    fields_list.append(_field("📰 국내 뉴스 키워드", "\n".join(lines)))
        except Exception as e:
            logger.debug(f"뉴스 리마인드 실패: {e}")

        # ── 4) 감시 현황 요약 ──
        cb_watching = storage.get_cb_watching()
        if cb_watching:
            fields_list.append(_field(f"👀 CB 감시 ({len(cb_watching)}건)",
                                     ", ".join(w.get("name", "?") for w in cb_watching[:10])))

        if fields_list:
            midday_embed = _embed("📊 장중 시그널 리포트", "", COLOR_BLUE, fields_list,
                                  footer="12:00 장중 체크")
            self.notifier.send_pado([midday_embed])
            logger.info(f"📊 장중 시그널 발송: {len(fields_list)}개 필드")
        else:
            logger.info("장중 시그널: 발송할 내용 없음")

    # ─────────────────────────────────────
    # 15:00 ClosingBell TOP3
    # ─────────────────────────────────────

    def run_cb_pick(self):
        """🎯 ClosingBell — 1차 감시 → 2차 눌림목 → 3차 재차거시 검증.

        1) 스코어링 → 기준 통과 전부 감시 등록
        2) CB 감시종목 전체에서 유목민 눌림목 체크
        3) 눌림목 발생 종목만 재차거시 검증 (DART+공매도+거래원+뉴스)
        4) PASS/WARN만 웹훅 발송
        """
        logger.info("── 15:00 ClosingBell ──")

        from closingbell.screener import CBScreener
        from closingbell.entry_watchlist import check_pullbacks
        from jaechageosi.formatter import format_midday_check, format_cb_status
        from shared.notifier import embed as _embed, field as _field, COLOR_GREEN, COLOR_YELLOW

        # 1) 스코어링 → 거래량 터진 종목 감시 등록
        screener = CBScreener(api=self.api)
        result = screener.run(date=self._today)
        stocks = result.get("stocks", [])

        if stocks:
            qualified = [s for s in stocks if s.get("score", 0) >= 35]
            saved = storage.save_cb_watch(qualified[:60], self._today)  # v5: 25→60
            logger.info(f"CB 감시 등록: {saved}건 (점수 35+, 전체 {len(stocks)}종목)")

        expired = storage.expire_cb_watch(max_days=5)
        if expired:
            logger.info(f"CB 감시 만료 (D+5): {expired}건")

        # 2) CB 감시종목에서 유목민 눌림목 체크
        cb_watching = storage.get_cb_watching()
        if not cb_watching:
            logger.info("CB 감시종목 0건 — 스킵")
            return

        logger.info(f"CB 눌림목 체크: {len(cb_watching)}건 대상")
        hits = check_pullbacks(cb_watching, api=self.api)

        if not hits:
            embed = format_cb_status(cb_watching, stocks[:5])
            self.notifier.send_cb([embed])
            logger.info(f"🎯 CB 감시 상태: {len(cb_watching)}건 대기 중")
            return

        # 3) 눌림목 발생 종목만 재차거시 3차 검증
        verified = []
        for h in hits:
            code = h["code"]
            name = h["name"]
            verdict = self._verify_pullback(code, name)
            h["verdict"] = verdict["grade"]       # PASS / WARN / REJECT
            h["verify_reasons"] = verdict["reasons"]
            if verdict["grade"] != "REJECT":
                verified.append(h)
            logger.info(f"  3차 검증 {name}: {verdict['grade']} — {', '.join(verdict['reasons'][:3])}")

            # DB 저장 (전부 — 승률 분석용)
            storage.save_pullback_signal(self._today, h, verdict)

        # 4) PASS/WARN만 발송
        embeds = []
        if verified:
            embeds.append(format_midday_check(verified))

        # 검증 결과 요약도 같이 발송
        verify_lines = []
        for h in hits:
            icon = {"PASS": "📌", "WARN": "👀", "REJECT": "❌"}.get(h.get("verdict", "?"), "?")
            verify_lines.append(
                f"{icon} **{h['name']}** ({h.get('verdict', '?')})\n"
                f"  {' · '.join(h.get('verify_reasons', [])[:3])}"
            )
        if verify_lines:
            verify_embed = _embed("🔍 3차 검증 결과", "", COLOR_GREEN,
                                  [_field("재차거시 판정", "\n".join(verify_lines))],
                                  footer="DART·공매도·거래원·뉴스 기반")
            embeds.append(verify_embed)

        if embeds:
            self.notifier.send_cb(embeds[:10])
            pass_count = sum(1 for h in verified if h.get("verdict") == "PASS")
            warn_count = sum(1 for h in verified if h.get("verdict") == "WARN")
            logger.info(f"🎯 ClosingBell 발송: PASS {pass_count} / WARN {warn_count}")
        else:
            embed = format_cb_status(cb_watching, stocks[:5])
            self.notifier.send_cb([embed])
            logger.info(f"🎯 CB: 눌림목 {len(hits)}건 전부 REJECT")

    def _verify_pullback(self, code: str, name: str) -> dict:
        """3차 재차거시 검증 — 눌림목 발생 종목에 대해서만 실행.

        Returns: {"grade": "PASS"|"WARN"|"REJECT", "reasons": [...]}
        """
        import time
        reasons = []
        warnings = 0
        rejects = 0

        # ── DART 공시 ──
        try:
            from checkers.dart_checker import check_dart
            dart = check_dart(code)
            grade = dart.get("grade", 4)
            if grade <= 1:
                rejects += 1
                reasons.append("❌ DART 극위험")
            elif grade <= 2:
                warnings += 1
                reasons.append("⚠️ DART 위험 공시")
            elif grade >= 5:
                reasons.append("✅ DART 호재")
            else:
                reasons.append("DART 중립")
        except Exception as e:
            reasons.append(f"DART 조회 실패")

        # ── 공매도 ──
        try:
            if self.api:
                time.sleep(0.3)
                shorts = self.api.get_short_selling(code, days=5)
                if shorts:
                    avg = sum(s.get("short_ratio", 0) for s in shorts) / len(shorts)
                    if avg >= 10:
                        rejects += 1
                        reasons.append(f"❌ 공매도 {avg:.1f}% (과열)")
                    elif avg >= 5:
                        warnings += 1
                        reasons.append(f"⚠️ 공매도 {avg:.1f}%")
                    else:
                        reasons.append(f"✅ 공매도 {avg:.1f}% (양호)")
        except Exception:
            reasons.append("공매도 조회 실패")

        # ── 거래원 (외인/기관) ──
        try:
            if self.api:
                time.sleep(0.3)
                trends = self.api.get_investor_trend(code, days=5)
                if trends:
                    foreign = sum(t.get("foreign", 0) for t in trends)
                    inst = sum(t.get("institution", 0) for t in trends)
                    if foreign > 0 and inst > 0:
                        reasons.append(f"✅ 외인+기관 순매수")
                    elif foreign > 0:
                        reasons.append(f"외인 순매수 ({foreign:+,})")
                    elif foreign < 0 and inst < 0:
                        warnings += 1
                        reasons.append(f"⚠️ 외인+기관 동반 순매도")
                    else:
                        reasons.append(f"수급 혼조")
        except Exception:
            reasons.append("수급 조회 실패")

        # ── 뉴스 ──
        news_summary = ""
        try:
            from checkers.news_checker import check_news
            news = check_news(name)
            items = news.get("items", [])
            if items:
                neg_count = sum(1 for n in items if any(kw in n.get("title", "")
                    for kw in ["상폐", "횡령", "배임", "감사의견", "관리종목", "환기종목"]))
                if neg_count >= 2:
                    rejects += 1
                    reasons.append(f"❌ 악재 뉴스 {neg_count}건")
                elif neg_count == 1:
                    warnings += 1
                    reasons.append(f"⚠️ 부정 뉴스 1건")
                else:
                    reasons.append(f"뉴스 {len(items)}건 (악재 없음)")
                # AI 분석용 뉴스 요약
                news_summary = " / ".join(n.get("title", "")[:40] for n in items[:5])
            else:
                reasons.append("뉴스 없음")
        except Exception:
            reasons.append("뉴스 조회 실패")

        # ── AI 종합 분석 (Gemini) ──
        ai_comment = ""
        try:
            from config import GEMINI_API_KEY, GEMINI_MODEL
            if GEMINI_API_KEY:
                import requests as _req
                # 수집된 데이터를 Gemini에게 전달
                context = f"""종목: {name} ({code})
DART 공시: {reasons[0] if reasons else '없음'}
공매도: {reasons[1] if len(reasons) > 1 else '없음'}
수급: {reasons[2] if len(reasons) > 2 else '없음'}
최근 뉴스: {news_summary or '없음'}"""

                prompt = (
                    "한국 주식 단타 트레이더의 관점에서 아래 종목 데이터를 분석해주세요.\n"
                    "규칙:\n"
                    "1. 2줄 이내로 핵심만 말할 것\n"
                    "2. 매수 관점에서 긍정/부정/중립 판단\n"
                    "3. 가장 주의할 리스크 1개 언급\n"
                    "4. 인사말/설명 없이 바로 분석만\n\n"
                    f"{context}"
                )
                resp = _req.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    if text:
                        ai_comment = text.strip()[:200]  # 200자 제한
                        reasons.append(f"🤖 {ai_comment}")
        except Exception as e:
            logger.debug(f"AI 분석 실패 {name}: {e}")

        # ── 판정 ──
        if rejects > 0:
            return {"grade": "REJECT", "reasons": reasons}
        elif warnings >= 2:
            return {"grade": "WARN", "reasons": reasons}
        else:
            return {"grade": "PASS", "reasons": reasons}

    # ─────────────────────────────────────
    # 15:40~ 스크리닝 파이프라인
    # ─────────────────────────────────────

    def run_screening_pipeline(self):
        """v5 파이프라인: OHLCV갱신 → 캐시 → 글로벌 → 성과 → 차트 → 거래량 → 시황 → 재차거시 → 뉴스."""
        logger.info("── 15:40 파이프라인 시작 ──")

        # ① OHLCV 갱신
        try:
            from updater.fdr_update import update_ohlcv_all, update_global
            update_global()
            update_ohlcv_all()
            logger.info("① OHLCV 갱신 완료")
        except Exception as e:
            logger.error(f"① OHLCV 갱신 실패: {e}")

        # ①-b v2: OHLCV 캐시 로드 (거래대금 사이드카 동시 수집)
        from shared.ohlcv_cache import OHLCVCache
        cache = OHLCVCache.instance()
        try:
            cache.preload_all()
            logger.info(f"①-b 캐시 로드 완료: {len(cache.get_all_codes())}종목, tv_sidecar {len(cache.tv_sidecar)}건")
        except Exception as e:
            logger.error(f"①-b 캐시 로드 실패: {e}")

        # ② 글로벌 지수
        try:
            from monitor.market_context import MarketContext
            MarketContext()
            logger.info("② 글로벌 지수 완료")
        except Exception as e:
            logger.warning(f"② 글로벌 실패: {e}")

        # ③ 성과 추적
        try:
            from monitor.performance_tracker import track_today
            track_today()
            logger.info("③ 성과 추적 완료")
        except Exception as e:
            logger.warning(f"③ 성과 추적 실패: {e}")

        # ④ 차트 스캔 (전종목, 캐시 사용)
        from jaechageosi.chart_engine import ChartEngine
        chart_engine = ChartEngine()
        chart_results = chart_engine.scan_all()
        logger.info(f"④ 차트 스캔: {len(chart_results)}건 후보")

        if not chart_results:
            logger.info("차트 후보 0건 — 파이프라인 종료")
            self._run_post_pipeline(cache)
            return

        # ⑤ 거래량 (차트 통과 종목만)
        from jaechageosi.volume_engine import VolumeEngine
        vol_engine = VolumeEngine(api=self.api)
        chart_vol_pass = []
        for cr in chart_results:
            vr = vol_engine.score_single(cr.code)
            if vr and vr.score >= 20:
                chart_vol_pass.append((cr, vr))

        logger.info(f"⑤ 차트+거래량 통과: {len(chart_vol_pass)}건")

        if not chart_vol_pass:
            logger.info("차트+거래량 통과 0건 — 파이프라인 종료")
            self._run_post_pipeline(cache)
            return

        # ⑥ 시황 (1회, v2: 캐시 tv_sidecar 전달)
        from jaechageosi.market_engine import MarketEngine
        market_engine = MarketEngine(api=self.api)
        market_result = market_engine.evaluate(self._today, tv_data=cache.tv_sidecar)
        logger.info(f"⑥ 시황: {market_result.mode} (점수 {market_result.score}, 테마 {market_result.leading_themes})")

        # 시황 DB 저장
        storage.save_market_daily({
            "date": self._today, "mode": market_result.mode,
            "leading_themes": market_result.leading_themes,
            "nasdaq_chg": market_result.nasdaq_change,
            "dangerous": market_result.dangerous, "score": market_result.score,
        })

        # ⑦ 재료 (통과 종목만, v2: sector 전달)
        from jaechageosi.material_engine import MaterialEngine
        from jaechageosi.intersection import intersect
        from jaechageosi.signal_book import SignalBook
        from shared.stock_map import get_stock

        mat_engine = MaterialEngine(api=self.api)
        book = SignalBook()
        all_results = []

        for cr, vr in chart_vol_pass:
            stock = get_stock(cr.code)
            if not stock:
                continue

            mr = mat_engine.evaluate(cr.code, stock.name, sector=stock.sector)

            # ⑧ 교집합
            result = intersect(cr, vr, mr, market_result, stock)
            all_results.append(result)

            if result.grade != "REJECT":
                logger.info(
                    f"  {result.grade} {stock.name} conf={result.confidence} "
                    f"C{cr.score}/V{vr.score}/M{mr.score}/Mk{market_result.score} "
                    f"theme+{result.theme_match_bonus}"
                )

        # ⑨ DB 저장 + 감시 등록
        saved = book.upsert_scan(self._today, all_results)
        logger.info(f"⑦⑧⑨ 재차거시 완료: {saved}건 저장")

        # ⑩ 파동 스캔 — 보류 (v5: 실전 효용 미확인)
        # from wave.detector import WaveDetector
        # detector = WaveDetector()
        # waves = detector.scan_all(self._today)
        logger.info("⑩ 파동 스캔: 보류")

        # ⑪ 주간 업데이트 (월요일만)
        if datetime.now().weekday() == 0:
            try:
                from updater.weekly_update import run_weekly_update
                run_weekly_update()
                logger.info("⑪ 주간 업데이트 완료")
            except Exception as e:
                logger.warning(f"⑪ 주간 실패: {e}")

        # ⑫ v3: 뉴스 인텔리전스 수집 (Google RSS + 네이버 확장)
        try:
            from checkers.news_intelligence import run_news_collection
            news_stats = run_news_collection(self._today)
            logger.info(f"⑫ 뉴스 수집 v3: {news_stats.get('total', 0)}건 (RSS {news_stats.get('google_rss', 0)} + 네이버 {news_stats.get('naver', 0)})")
        except Exception as e:
            logger.warning(f"⑫ 뉴스 v3 실패: {e}")
            # 폴백: 기존 수집
            try:
                from checkers.news_collector import collect_daily_news
                count = collect_daily_news(self._today)
                logger.info(f"⑫ 뉴스 레거시 폴백: {count}건")
            except Exception as e2:
                logger.warning(f"⑫ 뉴스 수집 전체 실패: {e2}")

        # ⑬ v3: 뉴스 분석 (델타 감지 + Gemini 키워드→종목 추론)
        try:
            from checkers.news_intelligence import run_news_analysis
            analysis = run_news_analysis(self._today)
            if analysis:
                themes = [t.get("keyword", "") for t in analysis.get("emerging_themes", [])[:3]]
                logger.info(f"⑬ 뉴스 분석: 부상 테마 {themes}, 시장 분위기 {analysis.get('market_mood', '?')}")
            else:
                logger.info("⑬ 뉴스 분석: 부상 키워드 없음")
        except Exception as e:
            logger.warning(f"⑬ 뉴스 분석 실패: {e}")

        logger.info("── 파이프라인 종료 ──")

    def _run_post_pipeline(self, cache=None):
        """파이프라인 조기 종료 시에도 실행해야 할 작업."""
        # ⑩ 파동 스캔 — 보류
        logger.info("⑩ 파동 스캔: 보류")

        # ⑫ 뉴스 수집 (매일 필수)
        try:
            from checkers.news_intelligence import run_news_collection
            news_stats = run_news_collection(self._today)
            logger.info(f"⑫ 뉴스 수집: {news_stats.get('total', 0)}건")
        except Exception as e:
            logger.warning(f"⑫ 뉴스 수집 실패: {e}")
            try:
                from checkers.news_collector import collect_daily_news
                collect_daily_news(self._today)
            except Exception:
                pass

        logger.info("── 파이프라인 종료 (조기) ──")

    # ─────────────────────────────────────
    # 전체 1회 실행
    # ─────────────────────────────────────

    def _is_trading_day(self) -> bool:
        """오늘이 거래일인지 판단 (주말 + KRX 공휴일)."""
        from datetime import date
        today = date.today()
        # 주말 체크
        if today.weekday() >= 5:  # 토(5), 일(6)
            return False
        # KRX 공휴일 체크
        try:
            import json
            from config import KRX_HOLIDAYS
            if KRX_HOLIDAYS.exists():
                with open(KRX_HOLIDAYS, "r", encoding="utf-8") as f:
                    holidays = json.load(f)
                if today.isoformat() in holidays:
                    return False
        except Exception:
            pass
        return True

    def run_morning(self):
        """아침 자동 실행 (08:25 트리거용).
        거래일: 브리핑 웹훅 (전일 결과)
        주말/공휴일: 뉴스 수집 + 글로벌 갱신
        """
        if self._is_trading_day():
            logger.info("=== 거래일 아침 — 브리핑 ===")
            self.run_morning_briefing()
        else:
            logger.info("=== 비거래일 — 뉴스 수집 ===")
            self.run_weekend()

    def run_once(self):
        """전체 파이프라인 1회 실행 (거래일 자동 판단)."""
        if self._is_trading_day():
            logger.info("=== 거래일 — 전체 파이프라인 ===")
            self.run_screening_pipeline()
            self.run_morning_briefing()
        else:
            logger.info("=== 비거래일 — 뉴스 수집만 ===")
            self.run_weekend()

    def run_test_all(self):
        """전체 스케줄 시뮬레이션 (주말에도 강제 실행, 웹훅 전부 발송)."""
        logger.info("=" * 50)
        logger.info("=== 전체 스케줄 테스트 (08:25→12:00→15:00→15:35) ===")
        logger.info("=" * 50)

        # 15:40 파이프라인 (먼저 — 데이터가 있어야 브리핑 가능)
        logger.info("\n── [15:40] 파이프라인 ──")
        self.run_screening_pipeline()

        # 08:20 아침 뉴스 수집 (v3)
        logger.info("\n── [08:20] 아침 뉴스 수집 ──")
        self._collect_morning_news()

        # 08:30 아침 브리핑 (전일 결과 + 파동)
        logger.info("\n── [08:30] 아침 브리핑 ──")
        self.run_morning_briefing()

        # 15:00 ClosingBell TOP3
        logger.info("\n── [15:00] ClosingBell TOP3 ──")
        self.run_cb_pick()

        # 14:00 장중 눌림목 (감시종목 있을 때만)
        logger.info("\n── [12:00] 장중 눌림목 체크 ──")
        self.run_midday_check()

        logger.info("=" * 50)
        logger.info("=== 전체 스케줄 테스트 완료 ===")
        logger.info("디스코드에서 웹훅 확인:")
        logger.info("  📰 아침 브리핑 (해외시황+외신+키워드)")
        logger.info("  📊 장중 시그널 (코스피/코스닥+국내뉴스)")
        logger.info("  🎯 ClosingBell (1차→2차→3차 검증)")
        logger.info("=" * 50)

    def run_weekend(self):
        """비거래일: 뉴스 수집 + 글로벌 지수 업데이트만."""
        # 글로벌 지수 (나스닥 금요일 종가 갱신)
        try:
            from updater.fdr_update import update_global
            update_global()
            logger.info("글로벌 지수 갱신 완료")
        except Exception as e:
            logger.warning(f"글로벌 갱신 실패: {e}")

        # 뉴스 수집 (매일 축적, v3 우선)
        try:
            from checkers.news_intelligence import run_news_collection
            stats = run_news_collection(self._today)
            logger.info(f"뉴스 수집 v3: {stats.get('total', 0)}건")
        except Exception as e:
            logger.warning(f"뉴스 v3 실패: {e}")
            try:
                from checkers.news_collector import collect_daily_news
                count = collect_daily_news(self._today)
                logger.info(f"뉴스 레거시: {count}건")
            except Exception as e2:
                logger.warning(f"뉴스 수집 실패: {e2}")

        logger.info("=== 비거래일 종료 ===")

    # ─────────────────────────────────────
    # 하루 자동 운영 (BAT 1개로 전부)
    # ─────────────────────────────────────

    def run_daily(self):
        """하루 전체 자동 운영. 08:20 기동 → 시간별 작업 → 완료 후 종료.

        거래일:
          08:20 기동 → 아침 뉴스 수집 (Google RSS + 네이버)
          08:25 → 아침 브리핑 (전일 결과 + 부상 키워드)
          12:00 대기 → 장중 눌림목
          15:00 대기 → ClosingBell TOP3
          15:35 대기 → 전체 파이프라인 (뉴스 추가 수집 포함)
          ~16:00 → 자동 종료

        비거래일:
          08:20 기동 → 뉴스 수집 + 글로벌 → 즉시 종료
        """
        import time as _time
        from datetime import datetime

        if not self._is_trading_day():
            logger.info("=" * 50)
            logger.info("=== 비거래일 — 뉴스 수집 후 종료 ===")
            logger.info("=" * 50)
            self.run_weekend()
            return

        logger.info("=" * 50)
        logger.info("=== 거래일 하루 운영 시작 ===")
        logger.info("=" * 50)

        # 작업 스케줄 (시:분, 함수, 설명)
        # v4.2: 하루 4알림 — 08:25 브리핑 / 12:00 장중 / 15:00 CB / 15:35 파이프라인(무발송)
        tasks = [
            ("08:20", self._collect_morning_news,  "📰 아침 뉴스 수집"),
            ("08:25", self.run_morning_briefing,   "📰 아침 브리핑"),
            ("12:00", self.run_midday_check,       "📊 장중 시그널"),
            ("15:00", self.run_cb_pick,            "🎯 ClosingBell"),
            ("15:35", self.run_screening_pipeline, "📊 장후 파이프라인"),
        ]

        now = datetime.now()
        executed = set()

        for sched_time, func, label in tasks:
            h, m = map(int, sched_time.split(":"))
            target = now.replace(hour=h, minute=m, second=0, microsecond=0)

            # 이미 지난 시간이면 즉시 실행 (늦게 켜진 경우)
            if datetime.now() > target:
                if sched_time not in executed:
                    logger.info(f"\n── [{sched_time}] {label} (지난 시간 — 즉시 실행) ──")
                    try:
                        func()
                    except Exception as e:
                        logger.error(f"{label} 실패: {e}")
                    executed.add(sched_time)
                continue

            # 대기
            wait = (target - datetime.now()).total_seconds()
            if wait > 0:
                logger.info(f"⏳ [{sched_time}] {label} 대기 중... ({wait/60:.0f}분 남음)")
                while datetime.now() < target:
                    _time.sleep(30)
                    # 1분마다 heartbeat
                    remaining = (target - datetime.now()).total_seconds()
                    if int(remaining) % 300 < 30:  # 5분마다 로그
                        logger.info(f"    ⏳ {remaining/60:.0f}분 남음")

            logger.info(f"\n── [{sched_time}] {label} ──")
            try:
                func()
            except Exception as e:
                logger.error(f"{label} 실패: {e}")
            executed.add(sched_time)

        logger.info("=" * 50)
        logger.info("=== 하루 운영 완료 — 종료 ===")
        logger.info(f"=== 실행된 작업: {len(executed)}개 ===")
        logger.info("=" * 50)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PADO v5 — 눌림목 단타 + 재차거시 검증")
    parser.add_argument("--once", action="store_true", help="전체 1회 실행")
    parser.add_argument("--morning", action="store_true", help="아침 (거래일=브리핑, 주말=뉴스)")
    parser.add_argument("--briefing", action="store_true", help="아침 브리핑만 (해외시황+외신+키워드)")
    parser.add_argument("--midday", action="store_true", help="장중 시그널만 (코스피/코스닥+국내뉴스)")
    parser.add_argument("--cb-pick", action="store_true", help="ClosingBell (1차→2차→3차 검증)")
    parser.add_argument("--scan", action="store_true", help="장후 파이프라인만")
    parser.add_argument("--weekend", action="store_true", help="비거래일 (뉴스+글로벌만)")
    parser.add_argument("--news", action="store_true", help="뉴스 수집만")
    parser.add_argument("--news-analyze", action="store_true", help="뉴스 수집 + 분석")
    parser.add_argument("--test-all", action="store_true", help="전체 스케줄 테스트")

    args = parser.parse_args()
    app = App()

    if args.test_all:
        app.run_test_all()
    elif args.morning:
        app.run_morning()
    elif args.once:
        app.run_once()
    elif args.briefing:
        app.run_morning_briefing()
    elif args.midday:
        app.run_midday_check()
    elif args.cb_pick:
        app.run_cb_pick()
    elif args.scan:
        app.run_screening_pipeline()
    elif args.weekend:
        app.run_weekend()
    elif args.news:
        from checkers.news_intelligence import run_news_collection
        stats = run_news_collection(app._today)
        print(f"뉴스 수집 완료: RSS {stats.get('google_rss',0)} + 네이버 {stats.get('naver',0)} = {stats.get('total',0)}건")
    elif args.news_analyze:
        from checkers.news_intelligence import run_news_collection, run_news_analysis
        stats = run_news_collection(app._today)
        print(f"수집: {stats.get('total',0)}건")
        result = run_news_analysis(app._today)
        if result:
            for t in result.get("emerging_themes", []):
                print(f"  🔮 {t.get('keyword','')} → {t.get('chain','')} → 종목: {t.get('stocks',[])}")
            print(f"  시장 분위기: {result.get('market_mood','?')}")
        else:
            print("부상 키워드 없음")
    else:
        app.run_daily()


if __name__ == "__main__":
    main()
