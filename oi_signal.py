def oi_divergence(oi_vals, price_vals, min_oi_growth=0.03, max_price_move=0.005):
    """
    OI растёт, цена стоит
    """
    if len(oi_vals) < 2 or len(price_vals) < 10:
        return False, 0

    oi_start = oi_vals[0]
    oi_end = oi_vals[-1]

    if oi_start <= 0:
        return False, 0

    oi_growth = (oi_end - oi_start) / oi_start

    price_start = price_vals[0]
    price_end = price_vals[-1]
    price_move = abs(price_end - price_start) / price_start

    if oi_growth >= min_oi_growth and price_move <= max_price_move:
        score = round(min(oi_growth * 100, 100), 1)
        return True, score

    return False, 0
