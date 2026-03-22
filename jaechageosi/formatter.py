"""디스코드 웹훅 메시지 포맷터 — 한글 친화적 (🎯🌊🔍📍)."""

from shared.notifier import embed, field, COLOR_GREEN, COLOR_YELLOW, COLOR_BLUE, COLOR_PURPLE

# ─────────────────────────────────────────────
# 내부 용어 → 한글 변환
# ─────────────────────────────────────────────

_CHART_STATE_KR = {
    "bottom": "🔻 바닥권",
    "pullback": "↩️ 눌림목",
    "breakout": "🚀 돌파",
    "extended": "🔥 과열",
}

_FLOW_STATE_KR = {
    "accumulation": "💰 자금축적",
    "ignite": "💥 거래량폭발",
    "digest": "😴 거래량급감",
    "reignite": "⚡ 재폭발",
    "chasing": "⚠️ 추격주의",
}

_GRADE_ICON = {
    "A": "🟢",
    "B": "🟡",
    "C": "🟠",
    "REJECT": "⚫",
}

_MODE_KR = {
    "theme_strong": "🔥 강한 테마장",
    "index_rally": "📈 지수 랠리",
    "mixed": "➡️ 혼조",
    "risk_off": "🛑 위험 회피",
}

_WAVE_KR = {
    "wave1": "🌊 1차파동 (바닥탈출)",
    "wave2": "🌊 2차파동 (재폭발)",
    "wave3": "🌊 3차파동 (약화)",
}

_STATUS_KR = {
    "watching": "👀 감시중",
    "entered": "✅ 진입",
    "exited": "📤 청산",
    "expired": "⏰ 만료",
}

def _kr(d: dict, key: str, fallback: str = "") -> str:
    return d.get(key, fallback or key)


def _build_channel_bar(c: int, v: int, m: int, mk: int, bonus: int, conf: int) -> str:
    """확신도를 녹색 바로 시각화 (10칸). 🟩=채움 ⬜=빈칸."""
    filled = max(1, min(10, round(conf / 10)))
    return "🟩" * filled + "⬜" * (10 - filled)


# ─────────────────────────────────────────────
# 🎯 ClosingBell TOP3
# ─────────────────────────────────────────────

def format_cb_pick(picks: list[dict], market_note: str = "") -> dict:
    desc = f"시황: {market_note}\n" if market_note else ""
    fields_list = []
    for i, p in enumerate(picks[:3], 1):
        name = p.get("name", "?")
        code = p.get("code", "")
        price = p.get("current_price", 0)
        support = p.get("support_line", 0)
        entry = p.get("entry_price", 0)
        stop = p.get("stop_loss", 0)
        target = p.get("target_price", 0)
        score = p.get("score", 0)
        rsi = p.get("rsi", "-")

        txt = f"**{score}점** | RSI {rsi}"
        if price:
            txt += f"\n💰 현재가 {price:,.0f}원"
        if entry and stop:
            risk = entry - stop if entry > stop else 0
            reward = target - entry if target > entry else 0
            rr = reward / risk if risk > 0 else 0
            txt += f"\n📌 진입: {entry:,.0f} | 🔴 손절: {stop:,.0f} | 🟢 목표: {target:,.0f}"
            if rr > 0:
                txt += f"\n손익비 1:{rr:.1f}"

        fields_list.append(field(
            f"{i}위 {name} ({code})",
            txt,
        ))
    return embed("🎯 ClosingBell TOP3", desc, COLOR_GREEN, fields_list,
                 footer="CB 감시종목 눌림목 포착")


def format_cb_status(watching: list[dict], today_top5: list[dict]) -> dict:
    """CB 감시 상태 메시지 — 눌림목 없을 때 발송."""
    desc = "눌림목 대기 중 — 조건 충족 시 알림"

    fields_list = []

    # 오늘 새로 등록된 TOP5
    if today_top5:
        lines = []
        for i, s in enumerate(today_top5[:5], 1):
            name = s.get("name", "?")
            score = s.get("score", 0)
            rsi = s.get("rsi", 0)
            pool = s.get("pool_type", "")
            pool_tag = " [C]" if pool == "core" else ""
            lines.append(f"  {i}. {name} {score}점 RSI {rsi:.0f}{pool_tag}")
        fields_list.append(field(
            f"📊 오늘 TOP5 (감시 등록)",
            "\n".join(lines),
        ))

    # 전체 감시 중
    if watching:
        lines = []
        for w in watching[:8]:
            name = w.get("name", "?")
            added = w.get("added_date", "")[-5:]
            lines.append(f"  👀 {name} ({added})")
        fields_list.append(field(
            f"📋 CB 감시 ({len(watching)}건)",
            "\n".join(lines),
        ))

    return embed("🎯 ClosingBell", desc, COLOR_GREEN, fields_list,
                 footer="눌림목 대기 중")


# ─────────────────────────────────────────────
# 🌊 파동 알림
# ─────────────────────────────────────────────

def format_wave_alert(signals: list[dict]) -> dict:
    fields_list = []
    for s in signals[:5]:
        wave_type = s.get("wave_type", "wave2")
        wave_label = _kr(_WAVE_KR, wave_type, "파동")
        name = s.get("name", "?")
        code = s.get("code", "")
        strength = s.get("strength", 0)
        count = s.get("wave_count", 1)

        # 강도 바
        filled = int(strength * 10)
        bar = "🟩" * filled + "⬜" * (10 - filled)

        # 이유 한글화
        reasons = s.get("reasons", [])
        reason_text = " · ".join(reasons[:3]) if reasons else ""

        fields_list.append(field(
            f"{wave_label} — {name} ({code})",
            f"강도: {bar} **{strength:.0%}**\n"
            f"{reason_text}\n"
            f"{'⚠️ 약화 주의' if count >= 3 else f'{count}차 감지'}",
        ))
    return embed("🌊 파동 감지", "**추격매수 금지** — 눌림목 대기", COLOR_PURPLE, fields_list,
                 footer="전일 장후 스캔 기준")


# ─────────────────────────────────────────────
# 🔍 재차거시 브리핑
# ─────────────────────────────────────────────

def format_morning_scan(data: dict, market=None) -> dict:
    fields_list = []

    # 시황 요약
    if market:
        mode = getattr(market, "mode", "mixed")
        mode_kr = _kr(_MODE_KR, mode)
        themes = getattr(market, "leading_themes", [])[:3]
        themes_str = ", ".join(themes) if themes else "불명확"
        nasdaq = getattr(market, "nasdaq_change", 0)
        score = getattr(market, "score", 0)
        seasonal = getattr(market, "seasonal_note", "")

        fields_list.append(field("📊 시황",
            f"**{mode_kr}** ({score}점)\n"
            f"오늘의 주도: **{themes_str}**\n"
            f"나스닥 {nasdaq:+.2f}%"
            + (f" | {seasonal}" if seasonal else "")))

    # A/B 등급 후보
    for r in data.get("scan_results", [])[:5]:
        grade = r.get("grade", "?")
        icon = _kr(_GRADE_ICON, grade, "⚪")
        name = r.get("name", "?")
        code = r.get("code", "")
        conf = r.get("confidence", 0)

        cs = _kr(_CHART_STATE_KR, r.get("chart_state", ""), "")
        fs = _kr(_FLOW_STATE_KR, r.get("flow_state", ""), "")

        c = r.get("chart_score", 0)
        v = r.get("volume_score", 0)
        m = r.get("material_score", 0)
        mk = r.get("market_score", 0)
        tm = r.get("theme_match", 0)
        sy = r.get("synergy", 0)

        # 4채널 기여도 바 (10칸)
        conf_bar = _build_channel_bar(c, v, m, mk, tm + sy, conf)

        bonus_parts = []
        if tm > 0:
            bonus_parts.append(f"🏷️ 테마 +{tm}")
        if sy > 0:
            bonus_parts.append(f"⚡ 시너지 +{sy}")
        bonus_str = " ".join(bonus_parts)

        body = (
            f"{conf_bar}\n"
            f"{cs} | {fs}\n"
            f"🟦차{c} 🟩거{v} 🟧재{m} 🔴시{mk}"
        )
        if bonus_str:
            body += f"\n{bonus_str}"

        fields_list.append(field(
            f"{icon} {grade}등급 {name} ({code}) — 확신도 {conf}",
            body,
        ))

    # 감시중
    watching = data.get("watching", [])
    if watching:
        lines = []
        for w in watching[:5]:
            name = w.get("name", "?")
            added = w.get("added_date", "")[-5:]  # MM-DD만
            status = _kr(_STATUS_KR, w.get("status", "watching"))
            lines.append(f"  {status} {name} ({added})")
        fields_list.append(field(
            f"📋 감시명부 ({len(watching)}건)",
            "\n".join(lines),
        ))

    return embed("🔍 재차거시 브리핑", "", COLOR_GREEN, fields_list,
                 footer="전일 파이프라인 결과 기준")


# ─────────────────────────────────────────────
# 📍 눌림목 진입 포착
# ─────────────────────────────────────────────

def format_midday_check(hits: list[dict]) -> dict:
    fields_list = []
    for h in hits[:3]:
        name = h.get("name", "?")
        code = h.get("code", "")
        grade = h.get("grade", "?")
        price = h.get("current_price", 0)
        support = h.get("support_line", 0)
        vol_ratio = h.get("vol_ratio_pct", 0)
        entry = h.get("entry_price", 0)
        stop = h.get("stop_loss", 0)
        target = h.get("target_price", 0)
        warning = h.get("warning", "")

        # 거래량 상태 한글화
        if vol_ratio <= 20:
            vol_label = f"📉 거래 극히 적음 ({vol_ratio:.0f}%)"
        elif vol_ratio <= 50:
            vol_label = f"📊 거래 적음 ({vol_ratio:.0f}%)"
        else:
            vol_label = f"📈 거래 보통 ({vol_ratio:.0f}%)"

        # 지지선과 현재가 관계
        gap = (price - support) / support * 100 if support > 0 else 0
        if gap <= 1:
            support_label = "✅ 지지선 터치"
        elif gap <= 3:
            support_label = "🔸 지지선 근접"
        else:
            support_label = f"↕️ 지지선 {gap:.1f}% 위"

        # 손익비 계산
        if entry and stop and target and entry > stop:
            risk = entry - stop
            reward = target - entry
            rr = reward / risk if risk > 0 else 0
            rr_label = f"손익비 1:{rr:.1f}" if rr > 0 else ""
        else:
            rr_label = ""

        txt = (
            f"💰 현재가 **{price:,.0f}원**\n"
            f"{support_label} (지지: {support:,.0f}원)\n"
            f"{vol_label}\n"
            f"─────────────\n"
            f"📌 진입: {entry:,.0f}원\n"
            f"🔴 손절: {stop:,.0f}원\n"
            f"🟢 목표: {target:,.0f}원"
        )
        if rr_label:
            txt += f" ({rr_label})"
        if warning:
            txt += f"\n⚠️ {warning}"

        fields_list.append(field(
            f"🎯 {name} ({code}) — {grade}등급",
            txt,
        ))

    return embed("📍 눌림목 진입 포착", "", COLOR_YELLOW, fields_list,
                 footer="키움 현재가 API 기준")
