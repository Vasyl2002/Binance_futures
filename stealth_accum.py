"""
Stealth Accumulation Score (0-100).
Накопление до анонса: OI растёт, цена тихая, топ-трейдеры в лонгах.
"""
from typing import Optional, Tuple, Dict, Any
import aiohttp

from binance_rest import (
    get_open_interest_hist,
    get_klines,
    get_top_long_short_account_ratio,
    get_top_long_short_position_ratio_hist,
    get_taker_long_short_ratio,
)

# --- Stealth Score thresholds (из спецификации) ---
STEALTH_4H_OI_MIN = 18.0      # OI Δ > 18% за 4h
STEALTH_4H_PRICE_MAX = 10.0   # price Δ < 10%
STEALTH_4H_LS_ACCOUNTS_MIN = 1.35  # Top L/S Accounts > 1.35

STEALTH_6H_OI_MIN = 25.0      # OI Δ > 25%
STEALTH_6H_PRICE_MAX = 12.0
STEALTH_6H_TAKER_MIN = 1.7    # Taker Buy/Sell > 1.7

STEALTH_12H_OI_MIN = 35.0
STEALTH_12H_LS_POS_MIN = 1.4  # L/S Positions > 1.4 + slope > 0

STEALTH_24H_OI_MIN = 50.0
STEALTH_24H_PRICE_MAX = 20.0

STEALTH_RSI_LO = 55
STEALTH_RSI_HI = 75
STEALTH_RSI_OVERBOUGHT = 78


def _oi_delta_pct(oi_data: list, bars_back: int = 1) -> Optional[float]:
    if not oi_data or len(oi_data) < bars_back + 1:
        return None
    old_val = float(oi_data[-(bars_back + 1)].get("sumOpenInterest", 0) or 0)
    new_val = float(oi_data[-1].get("sumOpenInterest", 0) or 0)
    if old_val <= 0:
        return None
    return (new_val - old_val) / old_val * 100.0


def _price_change_pct(klines: list, bars_back: int = 1) -> Optional[float]:
    if not klines or len(klines) < bars_back + 1:
        return None
    old_close = float(klines[-(bars_back + 1)][4])
    new_close = float(klines[-1][4])
    if old_close <= 0:
        return None
    return (new_close - old_close) / old_close * 100.0


def _rsi_from_klines(klines: list, period: int = 14) -> Optional[float]:
    """RSI from close prices. Need at least period+1 bars."""
    if not klines or len(klines) < period + 1:
        return None
    closes = [float(k[4]) for k in klines[-(period + 1):]]
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _ls_position_slope(hist: list) -> bool:
    """True if L/S ratio slope > 0 (растёт)."""
    if not hist or len(hist) < 2:
        return False
    prev = float(hist[-2].get("longShortRatio", 0) or 0)
    curr = float(hist[-1].get("longShortRatio", 0) or 0)
    return curr > prev


async def compute_stealth_score(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Tuple[float, Dict[str, Any]]:
    """
    Returns (score 0-100, details dict).
    Тянет данные для 4h, 6h, 12h, 24h и считает Stealth Accumulation Score.
    """
    details: Dict[str, Any] = {}
    score = 0.0

    # --- 4h ---
    oi_4h = await get_open_interest_hist(session, symbol, period="4h", limit=3)
    klines_4h = await get_klines(session, symbol, interval="4h", limit=3)
    ls_acc_4h = await get_top_long_short_account_ratio(session, symbol, period="4h", limit=2)

    if oi_4h and klines_4h:
        oi_d4 = _oi_delta_pct(oi_4h, 1)
        price_d4 = _price_change_pct(klines_4h, 1)
        details["oi_4h"] = oi_d4
        details["price_4h"] = price_d4
        if oi_d4 is not None and price_d4 is not None:
            if oi_d4 >= STEALTH_4H_OI_MIN and price_d4 < STEALTH_4H_PRICE_MAX:
                pts = min(35, 25 + (oi_d4 - STEALTH_4H_OI_MIN) / 5)
                score += pts
                details["4h_oi_pts"] = pts
    if ls_acc_4h is not None and ls_acc_4h >= STEALTH_4H_LS_ACCOUNTS_MIN:
        score += 15
        details["4h_ls_pts"] = 15
        details["ls_acc_4h"] = ls_acc_4h

    # --- 6h ---
    oi_6h = await get_open_interest_hist(session, symbol, period="6h", limit=3)
    klines_6h = await get_klines(session, symbol, interval="6h", limit=3)
    taker_6h = await get_taker_long_short_ratio(session, symbol, period="6h", limit=2)

    if oi_6h and klines_6h:
        oi_d6 = _oi_delta_pct(oi_6h, 1)
        price_d6 = _price_change_pct(klines_6h, 1)
        details["oi_6h"] = oi_d6
        details["price_6h"] = price_d6
        if oi_d6 is not None and price_d6 is not None:
            if oi_d6 >= STEALTH_6H_OI_MIN and price_d6 < STEALTH_6H_PRICE_MAX:
                pts = min(40, 30 + (oi_d6 - STEALTH_6H_OI_MIN) / 5)
                score += pts
                details["6h_oi_pts"] = pts
    if taker_6h is not None and taker_6h >= STEALTH_6H_TAKER_MIN:
        score += 20
        details["6h_taker_pts"] = 20
        details["taker_6h"] = taker_6h

    # --- 12h ---
    oi_12h = await get_open_interest_hist(session, symbol, period="12h", limit=3)
    ls_pos_12h = await get_top_long_short_position_ratio_hist(session, symbol, period="12h", limit=3)

    if oi_12h:
        oi_d12 = _oi_delta_pct(oi_12h, 1)
        details["oi_12h"] = oi_d12
        if oi_d12 is not None and oi_d12 >= STEALTH_12H_OI_MIN:
            score += 25
            details["12h_oi_pts"] = 25
    if ls_pos_12h and _ls_position_slope(ls_pos_12h):
        curr_ratio = float(ls_pos_12h[-1].get("longShortRatio", 0) or 0)
        details["ls_pos_12h"] = curr_ratio
        if curr_ratio >= STEALTH_12H_LS_POS_MIN:
            score += 20
            details["12h_ls_pts"] = 20

    # --- 24h ---
    oi_24h = await get_open_interest_hist(session, symbol, period="1d", limit=3)
    klines_24h = await get_klines(session, symbol, interval="1d", limit=3)

    if oi_24h and klines_24h:
        oi_d24 = _oi_delta_pct(oi_24h, 1)
        price_d24 = _price_change_pct(klines_24h, 1)
        details["oi_24h"] = oi_d24
        details["price_24h"] = price_d24
        if oi_d24 is not None and price_d24 is not None:
            if oi_d24 >= STEALTH_24H_OI_MIN and price_d24 < STEALTH_24H_PRICE_MAX:
                score += 20
                details["24h_pts"] = 20

    # --- RSI ---
    klines_1h = await get_klines(session, symbol, interval="1h", limit=20)
    rsi = _rsi_from_klines(klines_1h or [], 14) if klines_1h else None
    details["rsi"] = rsi
    if rsi is not None:
        if STEALTH_RSI_LO <= rsi <= STEALTH_RSI_HI:
            score += 10
            details["rsi_pts"] = 10
        elif rsi >= STEALTH_RSI_OVERBOUGHT:
            score -= 20
            details["rsi_pts"] = -20

    score = max(0.0, min(100.0, score))
    details["total"] = score
    return (score, details)
