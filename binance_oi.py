import aiohttp

BASE = "https://fapi.binance.com"

async def get_open_interest(session, symbol: str) -> float | None:
    url = f"{BASE}/futures/data/openInterestHist"
    params = {
        "symbol": symbol,
        "period": "5m",
        "limit": 2
    }

    async with session.get(url, params=params, timeout=10) as r:
        if r.status != 200:
            return None

        data = await r.json()
        if not isinstance(data, list) or len(data) < 2:
            return None

        return float(data[-1]["sumOpenInterest"])
