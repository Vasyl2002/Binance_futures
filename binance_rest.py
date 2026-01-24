# binance_rest.py
import aiohttp

BASE = "https://fapi.binance.com"

async def get_open_interest(session: aiohttp.ClientSession, symbol: str) -> float:
    url = f"{BASE}/fapi/v1/openInterest"
    async with session.get(url, params={"symbol": symbol}) as r:
        j = await r.json()
        # пример: {"openInterest":"12345.67","symbol":"BTCUSDT","time":...}
        oi = j.get("openInterest")
        if oi is None:
            raise ValueError(f"Bad OI response {symbol}: {j}")
        return float(oi)

async def get_funding(session: aiohttp.ClientSession, symbol: str) -> float:
    # premiumIndex содержит mark price и lastFundingRate
    url = f"{BASE}/fapi/v1/premiumIndex"
    async with session.get(url, params={"symbol": symbol}) as r:
        j = await r.json()
        fr = j.get("lastFundingRate")
        if fr is None:
            raise ValueError(f"Bad premiumIndex response {symbol}: {j}")
        return float(fr)
