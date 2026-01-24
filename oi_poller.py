# oi_poller.py
import asyncio
import aiohttp
import time
from state import STATES

BASE = "https://fapi.binance.com"

def _pick_candidates(allowed: set[str], *, ttl_sec: float = 15 * 60) -> list[str]:
    """
    Poll OI only for symbols that recently had a SETUP/SPike context.
    Sort by setup_score desc. Falls back to recent activity.
    """
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
    out.sort(reverse=True)  # by score then last
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

async def poll_oi(*, allowed: set[str], interval: float = 10.0, per_round: int = 40, ttl_sec: float = 15 * 60):
    """
    Polls open interest for a rotating subset of candidates.
    Uses /fapi/v1/openInterest.
    """
    cursor = 0
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        last_dbg_ts = 0.0
        last_total = -1
        while True:
            await asyncio.sleep(interval)
            now = time.time()

            candidates = _pick_candidates(allowed, ttl_sec=ttl_sec)
            batch, cursor = _rotate_batch(candidates, cursor, per_round)

            if not batch:
                continue

            # light debug
            if (now - last_dbg_ts) >= 60.0 or abs(len(candidates) - last_total) >= 10:
                print(f"[OI] tracking {len(candidates)} syms (interval={interval:.0f}s, per_round={per_round})")
                last_dbg_ts = now
                last_total = len(candidates)

            for sym in batch:
                st = STATES.get(sym)
                if st is None:
                    continue
                try:
                    url = f"{BASE}/fapi/v1/openInterest"
                    params = {"symbol": sym}
                    async with session.get(url, params=params) as r:
                        j = await r.json()
                    oi = j.get("openInterest")
                    if oi is None:
                        continue
                    ring = getattr(st, "oi", None)
                    if ring is None:
                        continue
                    ring.add(float(oi), ts=now)
                except Exception:
                    continue
