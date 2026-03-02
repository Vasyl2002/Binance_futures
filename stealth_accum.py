"""
Stealth + Squeeze Score (0-100).
Stealth: OI растёт, цена тихая (ESP/DOLO).
Squeeze: OI acceleration + price move (ORDER-тип).
Final score = max(Squeeze, Stealth)
"""
from typing import Optional, Tuple, Dict, Any, List
from collections import deque
import aiohttp

from binance_rest import (
    get_open_interest_hist,
    get_klines,
    get_top_long_short_account_ratio,
    get_top_long_short_position_ratio_hist,
    get_taker_long_short_ratio,
)

# --- Rolling storage: symbol -> {period -> deque of (oi_delta, price_delta, ts)} ---
_ROLLING: Dict[str, Dict[str, deque]] = {}
_ROLLING_MAX = 8
_OI_SPIKE_THRESH = 15.0  # раньше 20%, теперь ловим более ранние ускорения OI


def _get_rolling(symbol: str, period: str) -> deque:
    if symbol not in _ROLLING:
        _ROLLING[symbol] = {}
    if period not in _ROLLING[symbol]:
        _ROLLING[symbol][period] = deque(maxlen=_ROLLING_MAX)
    return _ROLLING[symbol][period]


def _oi_delta_pct(oi_data: list, bars_back: int = 1) -> Optional[float]:
    if not oi_data or len(oi_data) < bars_back + 1:
        return None
    old_val = float(oi_data[-(bars_back + 1)].get("sumOpenInterest", 0) or 0)
    new_val = float(oi_data[-1].get("sumOpenInterest", 0) or 0)
    if old_val <= 0:
        return None
    return (new_val - old_val) / old_val * 100.0


def _oi_acceleration(oi_data: list) -> Optional[float]:
    """acceleration = current_delta - prev_delta. Need 3+ bars."""
    if not oi_data or len(oi_data) < 3:
        return None
    prev_delta = _oi_delta_pct(oi_data[:-1], 1)
    curr_delta = _oi_delta_pct(oi_data, 1)
    if prev_delta is None or curr_delta is None:
        return None
    return curr_delta - prev_delta


def _price_change_pct(klines: list, bars_back: int = 1) -> Optional[float]:
    if not klines or len(klines) < bars_back + 1:
        return None
    old_close = float(klines[-(bars_back + 1)][4])
    new_close = float(klines[-1][4])
    if old_close <= 0:
        return None
    return (new_close - old_close) / old_close * 100.0


def _rsi_from_klines(klines: list, period: int = 14) -> Optional[float]:
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
    if not hist or len(hist) < 2:
        return False
    prev = float(hist[-2].get("longShortRatio", 0) or 0)
    curr = float(hist[-1].get("longShortRatio", 0) or 0)
    return curr > prev


# --- Squeeze thresholds (ORDER-тип) ---
SQUEEZE_4H_ACCEL_MIN = 16.0   # было 22.0 — делаем чувствительней
SQUEEZE_4H_PRICE_MIN = 3.0    # было 5.0 — дозволяем более ранний ход цены
SQUEEZE_6H_ACCEL_MIN = 20.0   # было 28.0
SQUEEZE_6H_TAKER_MIN = 1.6    # было 1.8
SQUEEZE_2H_ACCEL_MIN = 14.0   # было 20.0
SQUEEZE_12H_ACCEL_MIN = 0.0
SQUEEZE_24H_ACCEL_MIN = 0.0

# --- Stealth thresholds (ESP/DOLO) ---
# Делаем Stealth заметно чувствительней, чтобы ловить ранние накопления даже с умеренным OI.
STEALTH_4H_OI_MIN = 8.0       # раньше 15.0
STEALTH_4H_PRICE_MAX = 15.0   # позволяем чуть больший ход цены за 4h
STEALTH_6H_OI_MIN = 15.0      # раньше 22.0
STEALTH_6H_PRICE_MAX = 18.0
STEALTH_12H_OI_MIN = 25.0     # раньше 35.0
STEALTH_24H_OI_MIN = 30.0     # раньше 45.0
STEALTH_24H_PRICE_MAX = 30.0
STEALTH_4H_LS_ACCOUNTS_MIN = 1.25  # раньше 1.30
STEALTH_6H_TAKER_MIN = 1.5         # раньше 1.6
STEALTH_12H_LS_POS_MIN = 1.30      # немного мягче

# --- Early warning ---
EARLY_1H_ACCEL_MIN = 10.0    # ранний алерт по 1h/2h ускорению
EARLY_2H_ACCEL_MIN = 10.0

# --- Simple 5m OI+L/S early trigger (ужесточён, чтобы не спамить мелочь) ---
SIMPLE_5M_LIMIT = 8
SIMPLE_5M_OI_MIN_PCT = 0.5   # нужен хотя бы +0.5% OI за окно
SIMPLE_5M_POSITIVE_BARS = 2  # минимум 2 подряд растущих бара
SIMPLE_5M_LS_MIN = 0.9       # лёгкий перевес лонгов
# --- Fallback: рост OI 4h/6h → early_warning (теперь от 5%) ---
OI_FALLBACK_4H_PCT = 5.0     # OI 4h +5% → early_warning
OI_FALLBACK_6H_PCT = 5.0     # OI 6h +5% → early_warning
# --- Минимальный рост OI для любого early ---
OI_ANY_4H_PCT = 5.0          # OI 4h +5% → early_warning
OI_ANY_6H_PCT = 5.0          # OI 6h +5% → early_warning

# --- RSI ---
STEALTH_RSI_LO = 55
STEALTH_RSI_HI = 75
STEALTH_RSI_OVERBOUGHT = 78


async def compute_stealth_squeeze_score(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Tuple[float, Dict[str, Any]]:
    """
    Returns (score 0-100, details dict).
    score = max(Squeeze, Stealth)
    details["oi_spike_detected"] = True if acceleration > 20% on any window
    details["early_warning"] = True if 1h/2h accel > 15%
    """
    details: Dict[str, Any] = {}
    squeeze_score = 0.0
    stealth_score = 0.0
    oi_spike = False
    early_warning = False

    accel_1h = None
    accel_2h = None

    # --- Simple 5m OI + L/S (extra early flag) ---
    try:
        oi_5m = await get_open_interest_hist(session, symbol, period="5m", limit=SIMPLE_5M_LIMIT)
        # get_top_long_short_account_ratio возвращает уже float по последнему бару
        ls_5m = await get_top_long_short_account_ratio(session, symbol, period="5m", limit=2)
    except Exception:
        oi_5m, ls_5m = None, None

    if oi_5m and len(oi_5m) >= 4:
        # общий рост OI за окно
        simple_oi_pct = _oi_delta_pct(oi_5m, bars_back=min(3, len(oi_5m) - 1))
        # количество "зелёных" баров OI подряд в конце
        inc_streak = 0
        for i in range(len(oi_5m) - 1, 0, -1):
            prev_v = float(oi_5m[i - 1].get("sumOpenInterest", 0) or 0)
            cur_v = float(oi_5m[i].get("sumOpenInterest", 0) or 0)
            if prev_v > 0 and cur_v > prev_v:
                inc_streak += 1
            else:
                break
        ls_val = float(ls_5m) if ls_5m is not None else 1.0
        if (
            simple_oi_pct is not None
            and simple_oi_pct >= SIMPLE_5M_OI_MIN_PCT
            and inc_streak >= SIMPLE_5M_POSITIVE_BARS
            and ls_val >= SIMPLE_5M_LS_MIN
        ):
            details["simple_5m_oi_pct"] = simple_oi_pct
            details["simple_5m_inc_streak"] = inc_streak
            details["simple_5m_ls"] = ls_val
            early_warning = True

    # --- 1h (early warning) ---
    oi_1h = await get_open_interest_hist(session, symbol, period="1h", limit=4)
    if oi_1h and len(oi_1h) >= 3:
        accel_1h = _oi_acceleration(oi_1h)
        oi_d1h = _oi_delta_pct(oi_1h, 1)
        if oi_d1h is not None:
            details["oi_1h"] = oi_d1h
        if accel_1h is not None:
            details["accel_1h"] = accel_1h
            if accel_1h >= EARLY_1H_ACCEL_MIN:
                early_warning = True
            if accel_1h >= _OI_SPIKE_THRESH:
                oi_spike = True

    # --- 2h (early warning) ---
    oi_2h = await get_open_interest_hist(session, symbol, period="2h", limit=4)
    if oi_2h and len(oi_2h) >= 3:
        accel_2h = _oi_acceleration(oi_2h)
        oi_d2h = _oi_delta_pct(oi_2h, 1)
        if oi_d2h is not None:
            details["oi_2h"] = oi_d2h
        if accel_2h is not None:
            details["accel_2h"] = accel_2h
            if accel_2h >= EARLY_2H_ACCEL_MIN:
                early_warning = True
            if accel_2h >= _OI_SPIKE_THRESH:
                oi_spike = True

    oi_d4 = None
    accel_4h = None

    # --- 4h ---
    oi_4h = await get_open_interest_hist(session, symbol, period="4h", limit=4)
    klines_4h = await get_klines(session, symbol, interval="4h", limit=4)
    ls_acc_4h = await get_top_long_short_account_ratio(session, symbol, period="4h", limit=2)

    if oi_4h and len(oi_4h) >= 3:
        oi_d4 = _oi_delta_pct(oi_4h, 1)
        accel_4h = _oi_acceleration(oi_4h)
        details["oi_4h"] = oi_d4
        details["accel_4h"] = accel_4h
        if accel_4h is not None and accel_4h >= _OI_SPIKE_THRESH:
            oi_spike = True

    price_d4 = _price_change_pct(klines_4h or [], 1) if klines_4h else None
    details["price_4h"] = price_d4

    # Squeeze 4h
    if accel_4h is not None and price_d4 is not None:
        if accel_4h >= SQUEEZE_4H_ACCEL_MIN and price_d4 >= SQUEEZE_4H_PRICE_MIN:
            squeeze_score += 40
            details["squeeze_4h_pts"] = 40

    # Stealth 4h
    if oi_d4 is not None and price_d4 is not None:
        if oi_d4 >= STEALTH_4H_OI_MIN and price_d4 < STEALTH_4H_PRICE_MAX:
            stealth_score += 30
            details["stealth_4h_pts"] = 30
    if ls_acc_4h is not None and ls_acc_4h >= STEALTH_4H_LS_ACCOUNTS_MIN:
        stealth_score += 15
        details["stealth_4h_ls_pts"] = 15

    # --- 6h ---
    oi_6h = await get_open_interest_hist(session, symbol, period="6h", limit=4)
    klines_6h = await get_klines(session, symbol, interval="6h", limit=4)
    taker_6h = await get_taker_long_short_ratio(session, symbol, period="6h", limit=2)

    oi_d6 = None
    accel_6h = None
    if oi_6h and len(oi_6h) >= 3:
        oi_d6 = _oi_delta_pct(oi_6h, 1)
        accel_6h = _oi_acceleration(oi_6h)
        details["oi_6h"] = oi_d6
        details["accel_6h"] = accel_6h
        if accel_6h is not None and accel_6h >= _OI_SPIKE_THRESH:
            oi_spike = True

        # Squeeze 6h
        if accel_6h is not None and accel_6h >= SQUEEZE_6H_ACCEL_MIN and taker_6h is not None and taker_6h >= SQUEEZE_6H_TAKER_MIN:
            squeeze_score += 35
            details["squeeze_6h_pts"] = 35

        # Squeeze 2h (early window, Binance нет 8h)
        if accel_2h is not None and accel_2h >= SQUEEZE_2H_ACCEL_MIN:
            squeeze_score += 25
            details["squeeze_2h_pts"] = 25

    price_d6 = _price_change_pct(klines_6h or [], 1) if klines_6h else None
    details["price_6h"] = price_d6

    # Stealth 6h
    if oi_d6 is not None and price_d6 is not None:
        if oi_d6 >= STEALTH_6H_OI_MIN and price_d6 < STEALTH_6H_PRICE_MAX:
            stealth_score += 35
            details["stealth_6h_pts"] = 35
    if taker_6h is not None and taker_6h >= STEALTH_6H_TAKER_MIN:
        stealth_score += 20
        details["stealth_6h_taker_pts"] = 20

    # Fallback: рост OI 4h/6h → early_warning (чтобы не было нуля алертов)
    if oi_d4 is not None:
        if oi_d4 >= OI_FALLBACK_4H_PCT:
            early_warning = True
            details["oi_fallback_4h"] = oi_d4
        elif oi_d4 >= OI_ANY_4H_PCT:
            early_warning = True
            details["oi_any_4h"] = oi_d4
    if oi_d6 is not None:
        if oi_d6 >= OI_FALLBACK_6H_PCT:
            early_warning = True
            details["oi_fallback_6h"] = oi_d6
        elif oi_d6 >= OI_ANY_6H_PCT:
            early_warning = True
            details["oi_any_6h"] = oi_d6

    # --- 12h (fix oi_d12 for stealth) ---
    oi_12h = await get_open_interest_hist(session, symbol, period="12h", limit=4)
    ls_pos_12h = await get_top_long_short_position_ratio_hist(session, symbol, period="12h", limit=3)

    oi_d12 = None
    if oi_12h and len(oi_12h) >= 3:
        accel_12h = _oi_acceleration(oi_12h)
        oi_d12 = _oi_delta_pct(oi_12h, 1)
        details["oi_12h"] = oi_d12
        details["accel_12h"] = accel_12h
        if accel_12h is not None and accel_12h >= _OI_SPIKE_THRESH:
            oi_spike = True

        # Squeeze 12h
        if accel_12h is not None and accel_12h > SQUEEZE_12H_ACCEL_MIN:
            squeeze_score += 15
            details["squeeze_12h_pts"] = 15

        # Stealth 12h
        if oi_d12 is not None and oi_d12 >= STEALTH_12H_OI_MIN:
            stealth_score += 25
            details["stealth_12h_pts"] = 25
    if ls_pos_12h and _ls_position_slope(ls_pos_12h):
        curr_ratio = float(ls_pos_12h[-1].get("longShortRatio", 0) or 0)
        details["ls_pos_12h"] = curr_ratio
        if curr_ratio >= STEALTH_12H_LS_POS_MIN:
            stealth_score += 20
            details["stealth_12h_ls_pts"] = 20

    # --- 24h ---
    oi_24h = await get_open_interest_hist(session, symbol, period="1d", limit=4)
    klines_24h = await get_klines(session, symbol, interval="1d", limit=4)

    if oi_24h and len(oi_24h) >= 3:
        accel_24h = _oi_acceleration(oi_24h)
        oi_d24 = _oi_delta_pct(oi_24h, 1)
        price_d24 = _price_change_pct(klines_24h or [], 1) if klines_24h else None
        details["oi_24h"] = oi_d24
        details["price_24h"] = price_d24
        details["accel_24h"] = accel_24h
        if accel_24h is not None and accel_24h >= _OI_SPIKE_THRESH:
            oi_spike = True

        # Squeeze 24h
        if accel_24h is not None and accel_24h > SQUEEZE_24H_ACCEL_MIN:
            squeeze_score += 15
            details["squeeze_24h_pts"] = 15

        # Stealth 24h
        if oi_d24 is not None and price_d24 is not None:
            if oi_d24 >= STEALTH_24H_OI_MIN and price_d24 < STEALTH_24H_PRICE_MAX:
                stealth_score += 20
                details["stealth_24h_pts"] = 20

    # --- RSI ---
    klines_1h = await get_klines(session, symbol, interval="1h", limit=20)
    rsi = _rsi_from_klines(klines_1h or [], 14) if klines_1h else None
    details["rsi"] = rsi
    if rsi is not None:
        if STEALTH_RSI_LO <= rsi <= STEALTH_RSI_HI:
            stealth_score += 10
            details["rsi_pts"] = 10
        elif rsi >= STEALTH_RSI_OVERBOUGHT:
            stealth_score -= 20
            details["rsi_pts"] = -20

    score = max(squeeze_score, stealth_score)
    score = max(0.0, min(100.0, score))
    details["squeeze_score"] = squeeze_score
    details["stealth_score"] = stealth_score
    details["total"] = score
    details["oi_spike_detected"] = oi_spike
    details["early_warning"] = early_warning
    details["mode"] = "SQUEEZE" if squeeze_score >= stealth_score else "STEALTH"
    return (score, details)


# Backward compat
async def compute_stealth_score(
    session: aiohttp.ClientSession,
    symbol: str,
) -> Tuple[float, Dict[str, Any]]:
    """Alias for compute_stealth_squeeze_score."""
    return await compute_stealth_squeeze_score(session, symbol)
