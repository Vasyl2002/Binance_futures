# binance_ws.py
import asyncio
import json
import time
import websockets

from state import get_state, STATES

WS_URL = "wss://fstream.binance.com/ws/!ticker@arr"

DEBUG_WS = False
DEBUG_EVERY_SEC = 5

async def start_ws(*, allowed: set[str] | None = None):
    backoff = 1
    msg_cnt = 0
    last_dbg = 0.0

    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                print("[WS] Connected to Binance Futures ALL tickers")
                backoff = 1

                async for raw in ws:
                    msg_cnt += 1
                    now = time.time()

                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    if not isinstance(data, list):
                        continue

                    for i, d in enumerate(data):
                        sym = d.get("s")
                        if not sym:
                            continue
                        if allowed is not None and sym not in allowed:
                            continue

                        c = d.get("c")   # last price
                        q = d.get("q")   # quoteVolume 24h
                        P = d.get("P")   # priceChangePercent
                        if c is None or q is None:
                            continue

                        try:
                            price = float(c)
                            q24h = float(q)
                            p24h = float(P) if P is not None else 0.0
                        except Exception:
                            continue

                        st = get_state(sym)
                        st.price.add(price, ts=now)
                        st.q24h = q24h
                        st.p24h = p24h

                        # volume ring: дельта quoteVolume (приблизительно “приток” USDT)
                        last_q = getattr(st, "last_q", None)
                        last_q_ts = getattr(st, "last_q_ts", None)

                        if last_q is None:
                            st.last_q = q24h
                            st.last_q_ts = now
                        else:
                            dt = now - float(last_q_ts or now)
                            # если разрыв большой — просто ресет
                            if dt > 60:
                                st.last_q = q24h
                                st.last_q_ts = now
                            else:
                                delta = q24h - float(last_q)
                                if delta < 0:
                                    delta = 0.0
                                st.last_q = q24h
                                st.last_q_ts = now
                                st.volume.add(delta, ts=now)

                        if i % 250 == 0:
                            await asyncio.sleep(0)

                    if DEBUG_WS and (now - last_dbg) >= DEBUG_EVERY_SEC:
                        last_dbg = now
                        print(f"[WS DBG] msgs={msg_cnt} STATES={len(STATES)}")

        except Exception as e:
            print(f"[WS] crashed: {e} | reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


