# taker_poller.py
import asyncio
import aiohttp
import time
from state import STATES

BASE = "https://fapi.binance.com"

async def fetch_agg_trades(session: aiohttp.ClientSession, symbol: str, limit: int = 1000):
    url = f"{BASE}/fapi/v1/aggTrades"
    async with session.get(url, params={"symbol": symbol, "limit": limit}, timeout=10) as r:
        return await r.json()

def _calc_taker_ratio_from_trades(trades, since_ms: int):
    """
    aggTrades fields:
      p price, q qty, T time(ms), m bool (buyer is maker?)
    If m==True => buyer is maker => taker is seller => aggressive sell
    If m==False => aggressive buy
    """
    buy_q = 0.0
    sell_q = 0.0

    if not isinstance(trades, list):
        return None

    for t in trades:
        try:
            T = int(t.get("T", 0))
            if T < since_ms:
                continue
            price = float(t.get("p", 0.0))
            qty   = float(t.get("q", 0.0))
            quote = price * qty
            m = bool(t.get("m", False))
            if m:
                sell_q += quote
            else:
                buy_q += quote
        except Exception:
            continue

    tot = buy_q + sell_q
    if tot <= 0:
        return None

    ratio = buy_q / max(sell_q, 1e-9)             # >1 => агр. покупки сильнее
    net   = (buy_q - sell_q) / max(tot, 1e-9)     # -1..+1
    return ratio, net, tot

async def poll_taker(
    *,
    allowed: set[str],
    interval: float = 10.0,
    per_round: int = 12,
    setup_ttl_sec: int = 600,
    window_sec: int = 20,
):
    async with aiohttp.ClientSession() as session:
        while True:
            await asyncio.sleep(interval)
            now = time.time()
            now_ms = int(now * 1000)

            # только кандидаты
            syms = []
            for s, st in list(STATES.items()):
                if s not in allowed:
                    continue
                last = getattr(st, "setup_last_ts", 0.0)
                if (now - last) <= setup_ttl_sec:
                    syms.append(s)

            if not syms:
                continue

            batch = syms[:per_round]

            for sym in batch:
                st = STATES.get(sym)
                if st is None:
                    continue

                # чтобы считать именно последнее окно
                last_ms = getattr(st, "_taker_last_ms", 0)
                if last_ms <= 0:
                    st._taker_last_ms = now_ms - int(window_sec * 1000)
                    continue

                since_ms = now_ms - int(window_sec * 1000)
                # если вдруг интервал был большой — всё равно ограничим окном
                if last_ms > since_ms:
                    since_ms = last_ms

                try:
                    trades = await fetch_agg_trades(session, sym, limit=1000)
                    out = _calc_taker_ratio_from_trades(trades, since_ms=since_ms)
                    st._taker_last_ms = now_ms

                    if out is None:
                        continue

                    ratio, net, tot = out

                    # ratio и net можно оба хранить; минимум ratio достаточно
                    st.taker.add(float(ratio), ts=now)
                    # опционально:
                    st.taker_net = getattr(st, "taker_net", None) or None
                    if st.taker_net is not None:
                        st.taker_net.add(float(net), ts=now)

                except Exception:
                    continue
