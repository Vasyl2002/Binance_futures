# universe.py
import aiohttp

BASE = "https://fapi.binance.com"

async def load_usdt_perp_universe() -> set[str]:
    """
    Возвращает множество символов Binance USDT-M Perpetual (PERPETUAL) со статусом TRADING.
    Пример символа: "BTCUSDT"
    """
    url = f"{BASE}/fapi/v1/exchangeInfo"

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as r:
            j = await r.json()

    out: set[str] = set()
    for s in j.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        sym = s.get("symbol")
        if sym:
            out.add(sym)

    return out
