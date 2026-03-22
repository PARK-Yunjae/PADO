"""디스코드 웹훅 발송 (CB채널 + PADO채널 분리)."""

import json
import requests
from config import DISCORD_WEBHOOK_CB, DISCORD_WEBHOOK_PADO, API_TIMEOUT, setup_logging
from shared import storage

logger = setup_logging().getChild("notifier")


class Notifier:

    def send_cb(self, embeds: list[dict], content: str = "") -> bool:
        """🎯 ClosingBell 채널로 발송."""
        return self._send(DISCORD_WEBHOOK_CB, embeds, content, "cb")

    def send_pado(self, embeds: list[dict], content: str = "") -> bool:
        """🌊🔍📍 PADO 채널로 발송."""
        return self._send(DISCORD_WEBHOOK_PADO, embeds, content, "pado")

    def _send(self, url: str, embeds: list[dict], content: str, channel: str) -> bool:
        if not url:
            logger.warning(f"웹훅 URL 미설정: {channel}")
            return False

        payload = {"embeds": embeds[:10]}
        if content:
            payload["content"] = content[:2000]

        try:
            resp = requests.post(
                url, json=payload,
                headers={"Content-Type": "application/json"},
                timeout=API_TIMEOUT,
            )
            ok = resp.status_code in (200, 204)
            if not ok:
                logger.error(f"웹훅 실패 [{channel}] {resp.status_code}: {resp.text[:200]}")
            return ok
        except Exception as e:
            logger.error(f"웹훅 예외 [{channel}]: {e}")
            return False


# ─────────────────────────────────────────────
# 임베드 빌더 헬퍼
# ─────────────────────────────────────────────

COLOR_GREEN  = 0x34d399
COLOR_YELLOW = 0xfbbf24
COLOR_RED    = 0xf87171
COLOR_BLUE   = 0x60a5fa
COLOR_PURPLE = 0xa78bfa


def embed(title: str, description: str = "", color: int = COLOR_BLUE,
          fields: list[dict] | None = None, footer: str = "") -> dict:
    e = {"title": title, "color": color}
    if description:
        e["description"] = description[:4096]
    if fields:
        e["fields"] = fields[:25]
    if footer:
        e["footer"] = {"text": footer[:2048]}
    return e


def field(name: str, value: str, inline: bool = False) -> dict:
    return {"name": name[:256], "value": value[:1024], "inline": inline}
