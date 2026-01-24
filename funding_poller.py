# funding_poller.py
import asyncio
import aiohttp
import time
from state import STATES

BASE = "https://fapi.binance.com"

async def poll_funding(*, allowed: set[str], interval: float = 20.0):
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(interval)
            now = time.time()

            try:
                url = f"{BASE}/fapi/v1/premiumIndex"
                async with session.get(url, timeout=10) as r:
                    j = await r.json()
                if isinstance(j, dict):
                    j = [j]
                if int(time.time()) % 60 == 0:
                    print(f"[FUND] got {len(j)} items")

            except Exception:
                continue

            for item in j:
                try:
                    sym = item.get("symbol")
                    if not sym or sym not in allowed:
                        continue

                    st = STATES.get(sym)
                    if st is None:
                        continue

                    fr = item.get("lastFundingRate")
                    if fr is not None:
                        st.funding.add(float(fr), ts=now)

                    mark = float(item.get("markPrice", 0.0))
                    idx  = float(item.get("indexPrice", 0.0))
                    if idx > 0:
                        basis = (mark - idx) / idx * 100.0  # %
                        st.basis.add(float(basis), ts=now)

                except Exception:
                    continue


                mark = float(item.get("markPrice", 0.0))
                idx  = float(item.get("indexPrice", 0.0))
                basis = 0.0
                if idx > 0:
                    basis = (mark - idx) / idx * 100.0  # в %
                st.basis.add(basis, ts=now)

