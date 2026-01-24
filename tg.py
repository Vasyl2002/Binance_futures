# tg.py
import os
import time
import aiohttp

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

class RateLimiter:
    def __init__(self, per_minute: int = 20):
        self.min_interval = 60.0 / max(per_minute, 1)
        self.next_ts = 0.0

    def allow(self, now: float | None = None) -> bool:
        if now is None:
            now = time.time()
        if now >= self.next_ts:
            self.next_ts = now + self.min_interval
            return True
        return False

async def tg_send(text: str, *, token: str = TG_BOT_TOKEN, chat_id: str = TG_CHAT_ID) -> bool:
    if not token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as r:
                return r.status == 200
    except Exception:
        return False
