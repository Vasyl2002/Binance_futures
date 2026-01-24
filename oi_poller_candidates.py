# oi_poller.py  (или oi_poller_candidates.py)
import asyncio
import aiohttp
from candidates import list_sorted
from state import STATES
from binance_rest import get_open_interest, get_funding

async def poll_oi(interval_sec: int = 20, concurrency: int = 10):
    """
    Поллит OI (и funding) ТОЛЬКО для кандидатов (top-20).
    """
    timeout = aiohttp.ClientTimeout(total=10)
    sem = asyncio.Semaphore(concurrency)

    tick = 0

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            syms = list_sorted()
            if not syms:
                await asyncio.sleep(interval_sec)
                continue

            async def one_oi(sym: str):
                async with sem:
                    st = STATES.get(sym)
                    if st is None:
                        return
                    try:
                        oi = await get_open_interest(session, sym)
                        st.oi.add(oi)
                    except Exception:
                        return

            # OI для всех кандидатов
            await asyncio.gather(*(one_oi(s) for s in syms), return_exceptions=True)

            tick += 1

            # Funding реже (каждые ~60 сек при interval=20)
            if tick % 3 == 0:
                async def one_f(sym: str):
                    async with sem:
                        st = STATES.get(sym)
                        if st is None:
                            return
                        # если funding ring не добавлял — просто выйди
                        if not hasattr(st, "funding"):
                            return
                        try:
                            fr = await get_funding(session, sym)
                            st.funding.add(fr)
                        except Exception:
                            return

                await asyncio.gather(*(one_f(s) for s in syms), return_exceptions=True)

            await asyncio.sleep(interval_sec)
