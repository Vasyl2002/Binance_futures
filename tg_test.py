import asyncio
import os

import aiohttp


async def main() -> None:
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TG_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise SystemExit(
            "TG_BOT_TOKEN or TG_CHAT_ID is not set.\n"
            "Set them in your terminal, then run again."
        )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": "✅ TG test: scanner can send alerts.",
        "disable_web_page_preview": True,
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=15) as r:
            txt = await r.text()
            print(txt)


if __name__ == "__main__":
    asyncio.run(main())
