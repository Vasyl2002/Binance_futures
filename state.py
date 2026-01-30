# state.py
from buffers import Ring
import time


class TickerState:
    def __init__(self):
        # 300 точек — это НЕ "~60 секунд". Это зависит от частоты апдейтов.
        self.price   = Ring(300)
        self.volume  = Ring(300)
        self.oi      = Ring(120)
        self.funding = Ring(10)

        self.first_seen = time.time()
        self.last_q = None
        self.p24h = 0.0
        self.q24h = 0.0  # 24h quote volume (USDT)
        self.h24h = 0.0  # 24h high
        self.l24h = 0.0  # 24h low


        # антиспам SETUP (ступени)
        self.setup_bucket = 0
        self.setup_last_ts = 0.0
        self.setup_best_score = 0.0

        # антиспам ALERT (stage2)
        self.last_alert_ts = 0.0
        self.last_alert_level = "IGNORE"
        self.last_alert_score = 0.0

        self.funding = Ring(200)  # funding rate history

        # внутри TickerState
        self.taker = Ring(maxlen=600)   # ratio points
        self.basis = Ring(maxlen=600)   # basis points

        # hold-confirmation
        self.hold_ts = 0.0
        self.hold_side = None          # "LONG" / "SHORT"
        self.hold_trigger = 0.0
        self.hold_msg = None
        self.hold_rank = 0.0




STATES = {}

def get_state(symbol: str) -> TickerState:
    if symbol not in STATES:
        STATES[symbol] = TickerState()
    return STATES[symbol]
