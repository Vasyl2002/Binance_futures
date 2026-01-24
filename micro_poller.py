# micro_poller.py
import asyncio
import aiohttp
import time
from state import STATES

BASE = "https://fapi.binance.com"

def _pick_candidates(allowed: set[str], *, ttl_sec: float = 15 * 60) -> list[str]:
    now = time.time()
    out = []
    for sym in allowed:
        st = STATES.get(sym)
        if st is None:
            continue
        last = float(getattr(st, "setup_last_ts", 0.0))
        if last <= 0.0 or (now - last) > ttl_sec:
            continue
        score = float(getattr(st, "setup_score", 0.0))
        out.append((score, last, sym))
    out.sort(reverse=True)
    return [sym for _, _, sym in out]

def _rotate_batch(items: list[str], cursor: int, batch_size: int) -> tuple[list[str], int]:
    if not items:
        return [], 0
    n = len(items)
    cursor %= n
    batch = []
    i = cursor
    while len(batch) < min(batch_size, n):
        batch.append(items[i])
        i = (i + 1) % n
    return batch, i

def _ring_for(st, *names):
    for n in names:
        ring = getattr(st, n, None)
        if ring is not None:
            return ring
    return None

async def poll_taker_ratio(*, allowed: set[str], interval: float = 10.0, per_round: int = 25, period: str = "5m", ttl_sec: float = 15 * 60):
    """
    Poll taker buy/sell ratio using:
      GET /futures/data/takerlongshortRatio?symbol=...&period=...&limit=2
    Stores last buySellRatio into st.taker_ratio or st.taker.
    """
    cursor = 0
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            candidates = _pick_candidates(allowed, ttl_sec=ttl_sec)
            batch, cursor = _rotate_batch(candidates, cursor, per_round)
            if not batch:
                continue

            for sym in batch:
                st = STATES.get(sym)
                if st is None:
                    continue
                try:
                    url = f"{BASE}/futures/data/takerlongshortRatio"
                    params = {"symbol": sym, "period": period, "limit": 2}
                    async with session.get(url, params=params) as r:
                        j = await r.json()
                    if not isinstance(j, list) or not j:
                        continue
                    last = j[-1]
                    ratio = last.get("buySellRatio")
                    if ratio is None:
                        continue
                    ring = _ring_for(st, "taker_ratio", "taker")
                    if ring is None:
                        continue
                    ring.add(float(ratio), ts=now)
                except Exception:
                    continue

async def poll_basis(*, allowed: set[str], interval: float = 20.0, per_round: int = 25, period: str = "5m", ttl_sec: float = 15 * 60):
    """
    Poll basis (mark-index premium proxy) using:
      GET /futures/data/basis?symbol=...&period=...&contractType=PERPETUAL&limit=2
    Converts to percent: basis / futuresPrice * 100
    Stores into st.basis_pct or st.basis.
    """
    cursor = 0
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            candidates = _pick_candidates(allowed, ttl_sec=ttl_sec)
            batch, cursor = _rotate_batch(candidates, cursor, per_round)
            if not batch:
                continue

            for sym in batch:
                st = STATES.get(sym)
                if st is None:
                    continue
                try:
                    url = f"{BASE}/futures/data/basis"
                    params = {"symbol": sym, "period": period, "contractType": "PERPETUAL", "limit": 2}
                    async with session.get(url, params=params) as r:
                        j = await r.json()
                    if not isinstance(j, list) or not j:
                        continue
                    last = j[-1]
                    basis = last.get("basis")
                    fut = last.get("futuresPrice") or last.get("futuresPrice")
                    if basis is None or fut is None:
                        continue
                    fut = float(fut)
                    if fut == 0:
                        continue
                    basis_pct = float(basis) / fut * 100.0
                    ring = _ring_for(st, "basis_pct", "basis")
                    if ring is None:
                        continue
                    ring.add(float(basis_pct), ts=now)
                except Exception:
                    continue
