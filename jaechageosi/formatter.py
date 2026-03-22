"""디스코드 웹훅 메시지 포맷터 (🎯🌊🔍📍)."""

from shared.notifier import embed, field, COLOR_GREEN, COLOR_YELLOW, COLOR_BLUE, COLOR_PURPLE

# ─────────────────────────────────────────────

def format_cb_pick(picks: list[dict], market_note: str = "") -> dict:
    """🎯 15:00 ClosingBell TOP3."""
    desc = f"시황: {market_note}\n" if market_note else ""
    fields = []
    for i, p in enumerate(picks[:3], 1):
        name = p.get("name", "?")
        code = p.get("code", "")
        score = p.get("score", 0)
        rsi = p.get("rsi", "-")
        align = p.get("alignment", "-")
        note = p.get("note", "")
        fields.append(field(
            f"{i}위 {name} ({code})",
            f"**{score}점** | RSI {rsi} | {align}\n{note}",
        ))
    return embed("🎯 ClosingBell TOP3", desc, COLOR_GREEN, fields,
                 footer="15:00 기준 | 전일 종가 데이터")


def format_wave_alert(signals: list[dict]) -> dict:
    """🌊 08:30 파동 알림."""
    fields = []
    for s in signals[:5]:
        wave_type = "1차파동" if s.get("wave_type") == "wave1" else "2차파동"
        name = s.get("name", "?")
        code = s.get("code", "")
        strength = s.get("strength", 0)
        bar = "█" * int(strength * 10) + "░" * (10 - int(strength * 10))
        reasons = ", ".join(s.get("reasons", [])[:3])
        count = s.get("wave_count", 1)
        fields.append(field(
            f"■ {wave_type} — {name} ({code})",
            f"강도: {bar} {strength:.2f}\n{reasons}\n파동차수: {count}차",
        ))
    return embed("🌊 파동 감지", "", COLOR_PURPLE, fields,
                 footer="⚠️ 추격매수 금지 — 눌림목 대기 | 전일 장후 스캔")


def format_morning_scan(data: dict, market=None) -> dict:
    """🔍 08:30 재차거시 브리핑."""
    fields = []

    # 시황 요약
    if market:
        mode = getattr(market, "mode", "mixed")
        themes = ", ".join(getattr(market, "leading_themes", [])[:3]) or "불명확"
        nasdaq = getattr(market, "nasdaq_change", 0)
        seasonal = getattr(market, "seasonal_note", "")
        fields.append(field("📊 시황",
            f"**{mode.upper()}** | 주도: {themes}\n"
            f"나스닥 {nasdaq:+.2f}% | {seasonal}"))

    # A/B 등급 후보
    for r in data.get("scan_results", [])[:5]:
        grade = r.get("grade", "?")
        name = r.get("name", "?")
        code = r.get("code", "")
        conf = r.get("confidence", 0)
        cs = r.get("chart_state", "-")
        fs = r.get("flow_state", "-")
        c, v, m, mk = r.get("chart_score",0), r.get("volume_score",0), r.get("material_score",0), r.get("market_score",0)
        tm = r.get("theme_match", 0)
        sy = r.get("synergy", 0)
        icon = "🟢" if grade == "A" else "🟡"
        fields.append(field(
            f"{icon} {name} ({code}) — {grade}등급 conf {conf}",
            f"차트{c}({cs}) 거래량{v}({fs}) 재료{m} 시황{mk}\n"
            f"+{tm} 테마매칭 +{sy} 시너지",
        ))

    # 감시중
    watching = data.get("watching", [])
    if watching:
        lines = []
        for w in watching[:5]:
            name = w.get("name", "?")
            added = w.get("added_date", "")
            status = w.get("status", "watching")
            lines.append(f"{name} {added} {status}")
        fields.append(field(f"감시중 ({len(watching)}건)", "\n".join(lines)))

    return embed("🔍 재차거시 브리핑", "", COLOR_BLUE, fields,
                 footer="전일 파이프라인 결과 기준")


def format_midday_check(hits: list[dict]) -> dict:
    """📍 14:00 장중 눌림목."""
    fields = []
    for h in hits[:3]:
        name = h.get("name", "?")
        code = h.get("code", "")
        grade = h.get("grade", "?")
        price = h.get("current_price", 0)
        support = h.get("support_line", 0)
        vol_ratio = h.get("vol_ratio_pct", 0)
        entry = h.get("entry_price", "-")
        stop = h.get("stop_loss", "-")
        target = h.get("target_price", "-")
        warning = h.get("warning", "")

        txt = (f"현재가: {price:,.0f}원\n"
               f"지지선: {support:,.0f}원\n"
               f"거래량: MA20의 {vol_ratio:.0f}%\n"
               f"📌 진입: {entry} | 🔴 손절: {stop} | 🟢 목표: {target}")
        if warning:
            txt += f"\n⚠️ {warning}"
        fields.append(field(f"🎯 {name} ({code}) — {grade}등급", txt))

    return embed("📍 눌림목 진입 포착", "", COLOR_YELLOW, fields,
                 footer="14:00 키움 현재가 API 기준")
