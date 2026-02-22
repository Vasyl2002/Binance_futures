# binance_rest.py
import aiohttp
from typing import List, Optional

BASE = "https://fapi.binance.com"

async def get_open_interest_hist(
    session: aiohttp.ClientSession,
    symbol: str,
    period: str = "15m",
    limit: int = 30,
) -> Optional[List[dict]]:
    """Open Interest history. period: 5m,15m,30m,1h,2h,4h,6h,12h,1d"""
    url = f"{BASE}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": period, "limit": limit}
    async with session.get(url, params=params, timeout=15) as r:
        if r.status != 200:
            return None
        data = await r.json()
        if not isinstance(data, list) or len(data) < 2:
            return None
        return data


async def get_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str = "15m",
    limit: int = 30,
) -> Optional[List[list]]:
    """Klines: [open_time, open, high, low, close, volume, ...]"""
    url = f"{BASE}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with session.get(url, params=params, timeout=15) as r:
        if r.status != 200:
            return None
        data = await r.json()
        if not isinstance(data, list) or len(data) < 2:
            return None
        return data


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
