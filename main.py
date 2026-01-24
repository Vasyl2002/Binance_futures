import asyncio
from universe import load_usdt_perp_universe
from binance_ws import start_ws
from oi_poller import poll_oi
from funding_poller import poll_funding
from micro_poller import poll_taker_ratio, poll_basis
from scanner import Scanner
import os

async def main():
    # allowed = await load_usdt_perp_universe()
    allowed = set(await load_usdt_perp_universe())

    print(f"[UNIVERSE] loaded {len(allowed)} USDT PERP symbols")

    def _load_env_file(path: str):
        if not os.path.exists(path):
            print(f"[ENV] .env not found at: {path}")
            return
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")  # на случай кавычек
                os.environ.setdefault(k, v)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    _load_env_file(os.path.join(BASE_DIR, ".env"))

    print("[ENV] token=", "yes" if os.getenv("TG_BOT_TOKEN") else "no","chat=", "yes" if os.getenv("TG_CHAT_ID") else "no")

    scanner = Scanner(allowed)

    # --- TG startup ping (чтобы сразу проверить что доставка работает) ---
    if getattr(scanner, "tg_enabled", False):
        try:
            # вариант A: если в scanner.py есть метод _tg_send(self, msg)
            await scanner._tg_send("✅ Scanner started and TG is working")
            print("[TG] startup ping sent")
        except AttributeError:
            # вариант B: если у тебя функция tg_send(msg) где-то импортируется
            try:
                from tg import tg_send  # если у тебя есть tg.py
                await tg_send("✅ Scanner started and TG is working")
                print("[TG] startup ping sent (tg_send)")
            except Exception as e:
                print(f"[TG] startup ping failed (no sender found): {e}")
        except Exception as e:
            print(f"[TG] startup ping failed: {e}")

    async def refresh_universe(allowed_set: set[str], interval_sec: int = 300):
        while True:
            await asyncio.sleep(interval_sec)
            new = set(await load_usdt_perp_universe())

            add = new - allowed_set
            if add:
                allowed_set.update(add)  # <= важно: обновляем IN-PLACE
                try:
                    scanner.allowed.update(add)
                except Exception:
                    pass
                print(f"[UNIVERSE] +{len(add)} new symbols (total={len(allowed_set)})")




    await asyncio.gather(
        start_ws(allowed=allowed),
        # refresh_universe(),
        refresh_universe(allowed),
        poll_oi(allowed=allowed, interval=10, per_round=40),
        poll_funding(allowed=allowed, interval=20),
        poll_taker_ratio(allowed=allowed, interval=10, per_round=25, period="5m"),
        poll_basis(allowed=allowed, interval=20, per_round=25, period="5m"),
        scanner.run(),
    )


if __name__ == "__main__":
    asyncio.run(main())





