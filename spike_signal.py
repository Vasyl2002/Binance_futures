# spike_signal.py
import time

def spike_detector(
    *,
    price_ring,
    vol_ring,
    side: str,                 # "LONG" / "SHORT"
    short_sec: int = 20,
    long_sec: int = 180,
    min_move_pct: float = 0.35,
    min_vol_ratio: float = 3.0,
    min_vol_usdt: float = 30_000.0,
    min_close_pos: float = 0.65,   # анти-фитиль
):
    now = time.time()

    pw = price_ring.window(short_sec, now=now)
    if len(pw) < 3:
        return False, 0.0, 0.0, 0.0

    prices = [p for _, p in pw]
    p0 = prices[0]
    p1 = prices[-1]
    if p0 <= 0:
        return False, 0.0, 0.0, 0.0

    move_pct = (p1 - p0) / p0 * 100.0

    # --- anti-wick (закрылись не у экстремума) ---
    hi = max(prices)
    lo = min(prices)
    span = max(hi - lo, 1e-12)

    if side == "LONG":
        close_pos = (p1 - lo) / span  # 0..1
        if close_pos < min_close_pos:
            # всё равно вернём метрики, но ok=False
            pass
    else:  # SHORT
        close_pos = (hi - p1) / span  # 0..1
        if close_pos < min_close_pos:
            pass

    # --- volume ratio ---
    vs = vol_ring.window(short_sec, now=now)
    vl = vol_ring.window(long_sec, now=now)

    vol_short = sum(v for _, v in vs) if vs else 0.0
    vol_long  = sum(v for _, v in vl) if vl else 0.0

    # baseline = long - short (иначе long включает short и vr падает)
    base_vol = max(vol_long - vol_short, 0.0)
    base_sec = max(long_sec - short_sec, 1)

    rate_short = vol_short / max(short_sec, 1)
    rate_base  = base_vol / base_sec
    vol_ratio = rate_short / max(rate_base, 1e-12)

    # --- checks ---
    ok_dir = True
    if side == "LONG" and move_pct < min_move_pct:
        ok_dir = False
    if side == "SHORT" and move_pct > -min_move_pct:
        ok_dir = False



    ok_close = True
    if side == "LONG":
        ok_close = ((p1 - lo) / span) >= min_close_pos
    else:
        ok_close = ((hi - p1) / span) >= min_close_pos

    ok = ok_dir and ok_close and (vol_short >= min_vol_usdt) and (vol_ratio >= min_vol_ratio)

    return ok, move_pct, vol_ratio, vol_short
