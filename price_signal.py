# def price_compression(prices, threshold=0.004):
DEBUG = False



# def price_compression(prices, threshold=0.004):
# # def price_compression(prices, threshold=0.01 if DEBUG else 0.004):


#     """
#     prices: list[float]
#     threshold: 0.4% по умолчанию
#     """
#     if len(prices) < 10:
#         return False, 0.0

#     hi = max(prices)
#     lo = min(prices)

#     if lo <= 0:
#         return False, 0.0
    
#     mid = (hi + lo) / 2
#     if mid <= 0:
#         return False, 0.0

#     range_pct = (hi - lo) / mid

#     return (range_pct < threshold), range_pct


def price_compression(prices: list[float], min_points: int = 20):
    if len(prices) < min_points:
        return False, 0.0
    hi = max(prices)
    lo = min(prices)
    if lo <= 0:
        return False, 0.0
    rng = (hi - lo) / lo   # доля (например 0.003 = 0.3%)
    return True, rng
