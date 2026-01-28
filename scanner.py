import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Set
from collections import deque

import aiohttp

from state import STATES  # sym -> TickerState
from price_signal import price_compression
from volume_signal import volume_acceleration_time
from spike_signal import spike_detector


# ================== TUNING ==================
INTERVAL_SEC = 3.0

# --- Universe / exclusions ---
BLOCKLIST = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "BTCDOMUSDT"}
STABLE_LIKE = {"USDCUSDT", "TUSDUSDT", "FDUSDUSDT", "USDPUSDT", "DAIUSDT", "BUSDUSDT"}

# --- Stage 1: SETUP ---
SETUP_AGE_SEC = 45
SETUP_MIN_PRICE_POINTS = 20

MAX_RANGE_SETUP_PCT = 0.70
MAX_FAST_MOVE_SETUP_PCT = 0.35

MIN_Q24H_USDT = 5_000_000.0

VOL_SHORT_SEC = 15
VOL_LONG_SEC = 180
VOL_ACCEL_THRESH = 2.2
VOL_MIN_NOW = 20.0
VOL_MIN_BASE = 5.0
VOL_ACCEL_CAP = 50.0

BOOK_MIN_SCORE = 60.0
SETUP_PRINT_MIN_SCORE = 60.0
SETUP_COOLDOWN_SEC = 120

# --- Setup print throttling (console/TG) ---
# Prevents SETUP spam if the same symbol keeps meeting criteria with similar score.
SETUP_PRINT_COOLDOWN_SEC = 90
SETUP_PRINT_MIN_IMPROVE = 4.0
SETUP_BUCKET_STEP = 10

MODE = "BOTH"            # "LONG" | "SHORT" | "BOTH"
INCLUDE_NEUTRAL = False

# --- Stage 2/3: CONFIRM -> BREAKOUT ---
CONFIRM_MIN_OI_POINTS = 6
CONFIRM_MAX_RANGE_PCT = 1.00

BREAKOUT_LOOKBACK = 30
BREAKOUT_PAD_PCT = 0.03

# Breakout hold (to cut fakes)
HOLD_SEC = 15.0
HOLD_FAIL_RETRACE_PCT = 0.18  # invalidate pending breakout if retrace >= this % back inside

# --- SPIKE (as additional feature in Stage2 print line) ---
SPIKE_SHORT_SEC = 20
SPIKE_MIN_MOVE_PCT = 0.35
SPIKE_MIN_VOL_RATIO = 3.0
SPIKE_MIN_VOL_USDT = 30_000.0

# --- FUNDING delta for squeeze ---
FUNDING_LOOKBACK_SEC = 900
FUNDING_DELTA_MIN = 0.00003  # 0.003% in fraction

# --- OI for squeeze ---
OI_MIN_PCT_FOR_SQUEEZE = 0.8  # OI change over 60s

# --- Liquidations (from WS) ---
LIQ_WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"
LIQ_DEQUE_MAX = 800
LIQ_WINDOW_SEC = 45
LIQ_MIN_USDT = 150_000.0      # total liq notional in window (tune)
LIQ_IMB_MIN = 0.60            # imbalance (buy vs sell) to be directional

# ===== MOMO / IMPULSE (fast movers, no compression needed) =====
MOMO_LOOKBACK_SEC = 120          # смотрим импульс за 2 минуты
MOMO_CONFIRM_SEC = 20            # подтверждение что импульс “сейчас продолжается”
MOMO_MIN_MOVE_PCT =2.0          # минимум % за LOOKBACK
MOMO_CONFIRM_MIN_PCT = 0.6       # минимум % за CONFIRM (в ту же сторону)

MOMO_VOL_SHORT_SEC = 10
MOMO_VOL_LONG_SEC = 180
MOMO_VOL_ACCEL_THRESH = 3.5
MOMO_VOL_MIN_NOW = 150.0         # это “плотность” объёма, подстрой под себя
MOMO_VOL_MIN_BASE = 10.0

MOMO_TAKER_LONG_MIN = 1.10
MOMO_TAKER_SHORT_MAX = 0.90

MOMO_LIQ_WINDOW_SEC = 30
MOMO_COOLDOWN_SEC = 120          # анти-спам на 1 символ

# вверху scanner.py (рядом с остальными конфигами)
MOMO_POINTS = 18              # сколько последних апдейтов цены анализируем
MOMO_MIN_MOVE_PCT = 2.5       # для “пампа” подними 2.0–4.0
MOMO_VOL_ACCEL_THRESH = 6.0
MOMO_VOL_MIN_NOW = 500.0      # USDT за short окно (подстрой)
MOMO_COOLDOWN_SEC = 120


# --- Optional extra confirm signals ---
USE_TAKER_RATIO = True
TAKER_PERIOD = "5m"
TAKER_TTL_SEC = 60.0

USE_BASIS = True
BASIS_TTL_SEC = 30.0
BASIS_MIN_ABS_PCT = 0.02      # abs(mark-index)/index % threshold to consider futures pressure

# --- Alerts & Telegram batching ---
LEVEL_RANK = {"IGNORE": 0, "WATCH": 1, "HOT": 2, "VERY_HOT": 3}
ALERT_COOLDOWN = {"WATCH": 240, "HOT": 120, "VERY_HOT": 60}

PRINT_WATCH = False
PRINT_BREAKOUT_WATCH = True   # prints WATCH only if breakout_hold passed

# Telegram (optional): set env TG_BOT_TOKEN and TG_CHAT_ID (or pass in Scanner)
TG_SEND_TOP_N = int(os.getenv("TG_SEND_TOP_N", "3"))
TG_MIN_INTERVAL_SEC = float(os.getenv("TG_MIN_INTERVAL_SEC", "1.2"))


# ================== Small helpers ==================
def _pct_move(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def fmt_q24h(x: float) -> str:
    if x >= 1e9:
        return f"{x/1e9:.1f}B"
    if x >= 1e6:
        return f"{x/1e6:.1f}M"
    if x >= 1e3:
        return f"{x/1e3:.0f}K"
    return f"{x:.0f}"


def fmt_funding(fr: float) -> str:
    # funding is fraction (e.g. 0.0001 == 0.01%)
    return f"{fr*100:.4f}%"

def cached_latest(ring, default=0.0):
    if ring is None:
        return default
    try:
        vals = ring.values()
        if not vals:
            return default
        last = vals[-1]
        if isinstance(last, (tuple, list)) and len(last) == 2:
            return last[1]
        return last
    except Exception:
        return default


def fmt_float_opt(x: float | None, digits: int = 2) -> str:
    return "na" if x is None else f"{x:.{digits}f}"

def fmt_signed_pct_opt(x: float | None, digits: int = 3) -> str:
    return "na" if x is None else f"{x:+.{digits}f}%"


def ring_values(ring) -> List[float]:
    """Your Ring already supports .values(); keep it, but fallback if needed."""
    if ring is None:
        return []
    v = getattr(ring, "values", None)
    if callable(v):
        return list(v())
    # fallback: try iter of (ts,val)
    try:
        return [x[1] for x in list(ring)]
    except Exception:
        return []
    

def ring_window_prices(ring, sec: float, now: float):
    """
    Returns list[(ts, value)] for last `sec` seconds.
    Works whether Ring stores (ts,v) or just v.
    """
    sec = float(sec)

    # 1) Prefer ring.window if available (it is time-aware)
    if hasattr(ring, "window"):
        w = ring.window(sec, now=now)
        if not w:
            return []

        # If window already returns (ts, v)
        first = w[0]
        if isinstance(first, (tuple, list)) and len(first) == 2:
            return [(float(ts), float(v)) for ts, v in w]

        # If window returns only values -> synthesize timestamps
        n = len(w)
        if n == 1:
            return [(float(now), float(w[0]))]
        step = sec / (n - 1)
        return [(float(now - (n - 1 - i) * step), float(v)) for i, v in enumerate(w)]

    # 2) Fallback to ring.values
    vals = ring.values() if hasattr(ring, "values") else []
    if not vals:
        return []

    first = vals[0]
    if isinstance(first, (tuple, list)) and len(first) == 2:
        cutoff = now - sec
        out = []
        for ts, v in vals:
            if float(ts) >= cutoff:
                out.append((float(ts), float(v)))
        return out

    # vals are scalars -> synthesize timestamps
    n = len(vals)
    if n == 1:
        return [(float(now), float(vals[0]))]
    step = sec / (n - 1)
    return [(float(now - (n - 1 - i) * step), float(v)) for i, v in enumerate(vals)]




def ring_window(ring, sec: float, now: Optional[float] = None) -> List[Tuple[float, float]]:
    """
    Return [(ts,val), ...] inside last `sec`.
    Prefers ring.window(sec, now=now) if exists.
    """
    if ring is None:
        return []
    now = time.time() if now is None else float(now)
    w = getattr(ring, "window", None)
    if callable(w):
        try:
            return list(w(sec, now=now))
        except TypeError:
            # some impls might not accept now kw
            try:
                return list(w(sec))
            except Exception:
                return []
        except Exception:
            return []
    # fallback: if ring stores (ts,val) iterable
    out = []
    try:
        for ts, val in list(ring):
            if now - float(ts) <= sec:
                out.append((float(ts), float(val)))
        return out
    except Exception:
        return []


def direction_bias(prices: List[float]) -> Tuple[str, float, float]:
    """
    bias:
      pos  = where last is within range (0..1)
      move = micro-trend over last ~12 points
    """
    if len(prices) < 12:
        return "NEUTRAL", 0.0, 0.5

    hi = max(prices)
    lo = min(prices)
    last = prices[-1]
    pos = 0.5 if hi == lo else (last - lo) / (hi - lo)

    p0 = prices[-12]
    move_pct = _pct_move(p0, last)

    if move_pct > 0.05 and pos > 0.65:
        return "LONG", move_pct, pos
    if move_pct < -0.05 and pos < 0.35:
        return "SHORT", move_pct, pos
    return "NEUTRAL", move_pct, pos


def breakout_level_up(prices: List[float], lookback: int, pad_pct: float) -> Tuple[bool, float]:
    if len(prices) < lookback + 2:
        return False, 0.0
    last = prices[-1]
    prev_hi = max(prices[-(lookback + 1):-1])
    lvl = prev_hi * (1.0 + pad_pct / 100.0)
    return last >= lvl, lvl


def breakout_level_down(prices: List[float], lookback: int, pad_pct: float) -> Tuple[bool, float]:
    if len(prices) < lookback + 2:
        return False, 0.0
    last = prices[-1]
    prev_lo = min(prices[-(lookback + 1):-1])
    lvl = prev_lo * (1.0 - pad_pct / 100.0)
    return last <= lvl, lvl


def oi_pct_change_recent(oi_ring, sec: float = 60.0) -> float:
    w = ring_window(oi_ring, sec)
    if len(w) < 2:
        return 0.0
    a = w[0][1]
    b = w[-1][1]
    if a == 0:
        return 0.0
    return (b - a) / a * 100.0


def funding_delta_recent(funding_ring, sec: float = 900.0) -> Tuple[float, float, bool]:
    """
    Returns: (delta, now, flip_bool)
    funding values stored as FRACTION (0.0001 == 0.01%)
    """
    w = ring_window(funding_ring, sec)
    if len(w) < 2:
        vals = ring_values(funding_ring)
        now_val = float(vals[-1]) if vals else 0.0
        return 0.0, now_val, False

    a = float(w[0][1])
    b = float(w[-1][1])
    delta = b - a
    flip = (a <= 0 < b) or (a >= 0 > b)
    return float(delta), float(b), bool(flip)


def calc_setup_score(*, rng_pct: float, accel: float, bias: str, move_pct: float, pos: float) -> float:
    # 0..50: range compression
    compress_score = 50.0 * clamp((MAX_RANGE_SETUP_PCT - rng_pct) / MAX_RANGE_SETUP_PCT, 0.0, 1.0)
    # 0..30: volume accel
    vol_score = clamp(accel * 5.0, 0.0, 30.0)
    # 0..20: direction quality
    dir_score = 0.0
    if bias == "LONG":
        pos_bonus = 10.0 * clamp((pos - 0.65) / 0.35, 0.0, 1.0)
        tr_bonus = 10.0 * clamp((move_pct - 0.05) / 0.30, 0.0, 1.0)
        dir_score = pos_bonus + tr_bonus
    elif bias == "SHORT":
        pos_bonus = 10.0 * clamp((0.35 - pos) / 0.35, 0.0, 1.0)
        tr_bonus = 10.0 * clamp(((-move_pct) - 0.05) / 0.30, 0.0, 1.0)
        dir_score = pos_bonus + tr_bonus
    return float(compress_score + vol_score + dir_score)


# ================== Candidate book ==================
CANDIDATE_TTL_SEC = 10 * 60
CANDIDATE_TOP_N = 80


class CandidateBook:
    def __init__(self):
        self.data: Dict[str, Dict[str, float]] = {}  # sym -> {"score": float, "ts": float}

    def upsert(self, sym: str, score: float):
        now = time.time()
        self.data[sym] = {"score": float(score), "ts": now}

    def cleanup(self):
        now = time.time()
        dead = [s for s, v in self.data.items() if now - v["ts"] > CANDIDATE_TTL_SEC]
        for s in dead:
            self.data.pop(s, None)

    def top(self) -> List[str]:
        self.cleanup()
        items = sorted(self.data.items(), key=lambda kv: kv[1]["score"], reverse=True)
        return [sym for sym, _ in items[:CANDIDATE_TOP_N]]

    def get_score(self, sym: str) -> float:
        v = self.data.get(sym)
        return float(v["score"]) if v else 0.0

    def __len__(self):
        self.cleanup()
        return len(self.data)


# ================== Stats ==================
@dataclass
class StageStats:
    # stage1
    seen: int = 0
    not_allowed: int = 0
    too_young: int = 0
    q24h_missing: int = 0
    low_liq: int = 0
    p24h_hot: int = 0
    few_prices: int = 0
    no_compress: int = 0
    fast_move: int = 0
    vol_fail: int = 0
    bias_filtered: int = 0
    score_low: int = 0
    setup_suppressed: int = 0
    setup_printed: int = 0

    # stage2
    candidates: int = 0
    s2_p24h_hot: int = 0
    s2_few_prices: int = 0
    s2_no_compress: int = 0
    range_big: int = 0
    s2_vol_fail: int = 0
    few_oi: int = 0
    oi_not_growing: int = 0
    confirm_suppressed: int = 0
    confirm_printed: int = 0

    def reset(self):
        for k in self.__dict__.keys():
            setattr(self, k, 0)


# ================== Telegram helper ==================
class _RateLimiter:
    def __init__(self, min_interval_sec: float):
        self.min_interval = float(min_interval_sec)
        self._last = 0.0

    def allow(self, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else float(now)
        if now - self._last >= self.min_interval:
            self._last = now
            return True
        return False


async def _tg_send(http: aiohttp.ClientSession, token: str, chat_id: str, text: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    async with http.post(url, json=payload, timeout=10) as r:
        body = await r.text()
        if r.status != 200:
            print(f"[TG] send failed status={r.status} body={body[:300]}")
            return
        # иногда статус 200, но ok=false
        try:
            j = json.loads(body)
            if isinstance(j, dict) and (j.get("ok") is False):
                print(f"[TG] send ok=false: {body[:300]}")
        except Exception:
            pass



# ================== Scanner ==================
class Scanner:
    def __init__(
        self,
        allowed: Optional[Set[str]],
        *,
        tg_token: Optional[str] = None,
        tg_chat_id: Optional[str] = None,
        enable_liq_ws: bool = True,
    ):
        self.allowed = allowed
        self.book = CandidateBook()
        self.stats = StageStats()
        self._stats_ts = time.time()

        self._started = False

        # Telegram
        self.tg_token = tg_token or os.getenv("TG_BOT_TOKEN", "")
        self.tg_chat_id = tg_chat_id or os.getenv("TG_CHAT_ID", "")
        self.tg_enabled = bool(self.tg_token and self.tg_chat_id)
        print(f"[TG] enabled={self.tg_enabled}")
        print(f"[TG] enabled={self.tg_enabled} token={'yes' if self.tg_token else 'no'} chat={'yes' if self.tg_chat_id else 'no'}")
        self.tg_rl = _RateLimiter(TG_MIN_INTERVAL_SEC)

        # Liquidation WS
        self.enable_liq_ws = bool(enable_liq_ws)
        self._liq_task: Optional[asyncio.Task] = None

        # Caches (REST)
        self._taker_cache: Dict[str, Tuple[float, float]] = {}   # sym -> (ts, ratio)
        self._basis_cache: Dict[str, Tuple[float, float]] = {}   # sym -> (ts, basis_pct)

    def _print_stats_if_due(self):
        now = time.time()
        if now - self._stats_ts < 60:
            return
        self._stats_ts = now

        print("\n[STATS] last 60s")
        print(
            "  STAGE1:"
            f" seen={self.stats.seen} | not_allowed={self.stats.not_allowed} | too_young={self.stats.too_young}"
            f" | q24h_missing={self.stats.q24h_missing} | low_liq={self.stats.low_liq} | p24h_hot={self.stats.p24h_hot}"
            f" | few_prices={self.stats.few_prices} | no_compress={self.stats.no_compress} | fast_move={self.stats.fast_move}"
            f" | vol_fail={self.stats.vol_fail} | bias_filtered={self.stats.bias_filtered} | score_low={self.stats.score_low}"
            f" | setup_suppressed={self.stats.setup_suppressed} | setup_printed={self.stats.setup_printed}"
        )
        print(
            "  STAGE2:"
            f" candidates={len(self.book)} | p24h_hot={self.stats.s2_p24h_hot} | few_prices={self.stats.s2_few_prices}"
            f" | no_compress={self.stats.s2_no_compress} | range_big={self.stats.range_big} | vol_fail={self.stats.s2_vol_fail}"
            f" | few_oi={self.stats.few_oi} | oi_not_growing={self.stats.oi_not_growing}"
            f" | confirm_suppressed={self.stats.confirm_suppressed} | confirm_printed={self.stats.confirm_printed}"
        )
        print()
        self.stats.reset()

    def _momo_impulse(self, sym: str, st, now: float):
        last_ts = float(getattr(st, "momo_last_ts", 0.0))
        if now - last_ts < MOMO_COOLDOWN_SEC:
            return (False, "", 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)

        pts = ring_window_prices(st.price, MOMO_LOOKBACK_SEC, now)
        if len(pts) < 8:
            return (False, "", 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)

        p0 = pts[0][1]
        p1 = pts[-1][1]

        if p0 <= 0:
            return (False, "", 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)

        move_pct = (p1 / p0 - 1.0) * 100.0

        if abs(move_pct) < MOMO_MIN_MOVE_PCT:
            return (False, "", move_pct, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)

        
                # confirm: импульс должен продолжаться "прямо сейчас"
        cpts = ring_window_prices(st.price, MOMO_CONFIRM_SEC, now)
        if len(cpts) >= 4:
            c0 = cpts[0][1]
            c1 = cpts[-1][1]
            if c0 > 0:
                c_move = (c1 / c0 - 1.0) * 100.0
                if move_pct > 0 and c_move < MOMO_CONFIRM_MIN_PCT:
                    return (False, "", move_pct, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)
                if move_pct < 0 and c_move > -MOMO_CONFIRM_MIN_PCT:
                    return (False, "", move_pct, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)

        
        # anti-chase: если уже откатили от локального пика/дна — не шлём
        vals = [v for _, v in pts]  # <-- цены из твоего time-window
        window = vals[-MOMO_POINTS:] if len(vals) >= MOMO_POINTS else vals
        hi = max(window)
        lo = min(window)

        if move_pct > 0:
            dd = (hi - p1) / hi * 100.0 if hi > 0 else 0.0
            if dd >= 0.35:   # откат от локального хая
                return (False, "", move_pct, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)
        else:
            bounce = (p1 - lo) / lo * 100.0 if lo > 0 else 0.0
            if bounce >= 0.35:  # отскок от локального дна
                return (False, "", move_pct, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0)



        # volume accel (у тебя volume ring, судя по всему, time-based и работает)
        ok_vol, accel, v_now, v_base = volume_acceleration_time(
            st.volume,
            short_sec=10,
            long_sec=180,
            accel_thresh=MOMO_VOL_ACCEL_THRESH,
            min_now=MOMO_VOL_MIN_NOW,
            min_base=10.0,
            accel_cap=VOL_ACCEL_CAP,
        )
        if not ok_vol:
            return (False, "", move_pct, accel, v_now, v_base, 1.0, 0.0, 0.0)

        # taker/basis/liqs (берём кэш)
        taker = float(cached_latest(getattr(st, "taker_ratio", None), default=1.0))
        basis = float(cached_latest(getattr(st, "basis", None), default=0.0))
        liq_total = float(getattr(st, "liq_60s_usdt", 0.0))  # или как у тебя поле называется

        st.momo_last_ts = now
        side = "LONG" if move_pct > 0 else "SHORT"
        tag = "MOMO_UP" if side == "LONG" else "MOMO_DOWN"

        return (True, tag, move_pct, accel, v_now, v_base, taker, basis, liq_total)

        

    # ---------- Liquidation tracking ----------
    def _liq_deque(self, st) -> deque:
        d = getattr(st, "_liq_deque", None)
        if d is None:
            d = deque(maxlen=LIQ_DEQUE_MAX)
            setattr(st, "_liq_deque", d)
        return d

    def _liq_add(self, sym: str, ts: float, side: str, notional_usdt: float):
        st = STATES.get(sym)
        if st is None:
            return
        d = self._liq_deque(st)
        # BUY liquidation -> short liquidations -> pushes UP
        # SELL liquidation -> long liquidations -> pushes DOWN
        sign = +1.0 if str(side).upper() == "BUY" else -1.0
        d.append((float(ts), sign * float(notional_usdt)))

    def _liq_window_stats(self, st, sec: float, now: Optional[float] = None) -> Tuple[float, float, float, float]:
        """
        Returns (buy_usdt, sell_usdt, total_usdt, imbalance)
        imbalance = (buy - sell) / total
        """
        now = time.time() if now is None else float(now)
        d = self._liq_deque(st)
        buy = 0.0
        sell = 0.0
        total = 0.0

        while d and (now - d[0][0] > sec):
            d.popleft()

        for ts, signed in d:
            v = float(signed)
            if v >= 0:
                buy += v
            else:
                sell += -v
            total += abs(v)

        imb = 0.0 if total <= 0 else (buy - sell) / total
        return buy, sell, total, imb

    async def _liq_ws_loop(self):
        backoff = 1.0
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(LIQ_WS_URL, heartbeat=30) as ws:
                        print("[LIQ WS] connected (!forceOrder@arr)")
                        backoff = 1.0
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                continue

                            o = data.get("o") if isinstance(data, dict) else None
                            if not isinstance(o, dict):
                                continue

                            sym = o.get("s")
                            if not sym or (self.allowed is not None and sym not in self.allowed):
                                continue
                            if sym in BLOCKLIST or sym in STABLE_LIKE:
                                continue

                            side = o.get("S")  # BUY/SELL
                            q = o.get("q")
                            ap = o.get("ap") or o.get("p")
                            try:
                                notional = float(q) * float(ap)
                            except Exception:
                                continue

                            ts = float(data.get("E", time.time() * 1000.0)) / 1000.0
                            self._liq_add(sym, ts, side, notional)

            except Exception:
                pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 20.0)

    # ---------- REST helpers (cached) ----------
    async def _get_taker_ratio(self, session: aiohttp.ClientSession, sym: str) -> float:
        if not USE_TAKER_RATIO:
            return 0.0
        now = time.time()
        ts_val = self._taker_cache.get(sym)
        if ts_val and (now - ts_val[0] <= TAKER_TTL_SEC):
            return float(ts_val[1])

        url = "https://fapi.binance.com/futures/data/takerlongshortRatio"
        params = {"symbol": sym, "period": TAKER_PERIOD, "limit": 2}
        try:
            async with session.get(url, params=params, timeout=8) as r:
                j = await r.json()
            if not isinstance(j, list) or not j:
                return 0.0
            last = j[-1]
            ratio = float(last.get("buySellRatio", 0.0))
            self._taker_cache[sym] = (now, ratio)
            return ratio
        except Exception:
            return 0.0

    async def _get_basis_pct(self, session: aiohttp.ClientSession, sym: str) -> float:
        if not USE_BASIS:
            return 0.0
        now = time.time()
        ts_val = self._basis_cache.get(sym)
        if ts_val and (now - ts_val[0] <= BASIS_TTL_SEC):
            return float(ts_val[1])

        url = "https://fapi.binance.com/fapi/v1/premiumIndex"
        params = {"symbol": sym}
        try:
            async with session.get(url, params=params, timeout=8) as r:
                j = await r.json()
            mark = float(j.get("markPrice", 0.0))
            idx = float(j.get("indexPrice", 0.0))
            if idx <= 0:
                return 0.0
            basis_pct = (mark - idx) / idx * 100.0
            self._basis_cache[sym] = (now, basis_pct)
            return float(basis_pct)
        except Exception:
            return 0.0

    # ---------- Breakout HOLD state machine ----------
    def _breakout_hold_ok(self, st, bias: str, brk_now: bool, level: float, last_price: float, now: float) -> Tuple[bool, float]:
        """
        Returns (hold_ok, hold_age_sec)
        """
        dir_key = "UP" if bias == "LONG" else "DOWN"
        pend_dir = getattr(st, "_brk_pend_dir", None)
        pend_ts = float(getattr(st, "_brk_pend_ts", 0.0))
        pend_level = float(getattr(st, "_brk_pend_level", 0.0))

        if brk_now and (pend_dir != dir_key):
            st._brk_pend_dir = dir_key
            st._brk_pend_ts = now
            st._brk_pend_level = float(level)
            return False, 0.0

        if pend_dir == dir_key and pend_ts > 0:
            age = now - pend_ts

            if bias == "LONG":
                if last_price < pend_level * (1.0 - HOLD_FAIL_RETRACE_PCT / 100.0):
                    st._brk_pend_ts = 0.0
                    return False, 0.0
            else:
                if last_price > pend_level * (1.0 + HOLD_FAIL_RETRACE_PCT / 100.0):
                    st._brk_pend_ts = 0.0
                    return False, 0.0

            if age >= HOLD_SEC:
                return True, age
            return False, age

        return False, 0.0
    

    # ---------- Telegram gating ----------
    def _should_send_tg(self, level: str, tag: str, oi_pct: float, sp_vr: float, liq_total: float) -> bool:

        if not self.tg_enabled:
            return False
        
        if tag.startswith("MOMO"):
            return True
        
        if tag.startswith("FADE"):
            return True

        # Сигналы "высокого доверия" — шлём всегда (но анти-спам/rl всё равно сработает ниже по коду)
        if tag.startswith("SQUEEZE") or tag.startswith("LIQ"):
            return True

        # Breakout + удержание уровня — это как раз то, ради чего сканер нужен
        if tag.startswith("BREAKOUT_HOLD"):
            return True

                # For breakouts: allow HOT/VERY_HOT if there's "fuel"
        if tag.startswith("BREAKOUT") and level in ("HOT", "VERY_HOT"):
            if abs(oi_pct) >= 1.2 or sp_vr >= 4.0 or liq_total >= (LIQ_MIN_USDT * 1.2):
                return True

        # Otherwise: keep VERY_HOT strict rule
        if level == "VERY_HOT":
            if abs(oi_pct) >= 1.5 or sp_vr >= 5.0 or liq_total >= (LIQ_MIN_USDT * 1.2):
                return True


        return False

    async def run(self):
        if self._started:
            return
        self._started = True

        print("[SCANNER] started (setup -> confirm -> breakout_hold + squeeze + liq)")

        if self.enable_liq_ws and self._liq_task is None:
            self._liq_task = asyncio.create_task(self._liq_ws_loop())

        async with aiohttp.ClientSession() as http:
            if self.tg_enabled:
                try:
                    await _tg_send(http, self.tg_token, self.tg_chat_id, "✅ Scanner started. TG OK.")
                    print("[TG] startup ping sent")
                except Exception as e:
                    print(f"[TG] startup ping error: {e!r}")
                    print(f"[TG] send failed: {e}")
            while True:
                await asyncio.sleep(INTERVAL_SEC)
                self._print_stats_if_due()

                now = time.time()
                items = list(STATES.items())
                signals: List[Tuple[float, str]] = []

                # ========== STAGE 1: SETUP ==========
                for sym, st in items:
                    self.stats.seen += 1

                    if self.allowed is not None and sym not in self.allowed:
                        self.stats.not_allowed += 1
                        continue
                    if not sym.endswith("USDT"):
                        continue
                    if sym in BLOCKLIST or sym in STABLE_LIKE:
                        continue

                    age = now - getattr(st, "first_seen", now)
                    if age < SETUP_AGE_SEC:
                        self.stats.too_young += 1
                        continue

                    q24h = getattr(st, "q24h", None)
                    if q24h is None:
                        self.stats.q24h_missing += 1
                        continue
                    if float(q24h) < MIN_Q24H_USDT:
                        self.stats.low_liq += 1
                        continue

                    prices = ring_values(st.price)
                    if len(prices) < SETUP_MIN_PRICE_POINTS:
                        self.stats.few_prices += 1
                        continue

                    # --- MOMO / IMPULSE (ловим сильные пампы/дампы без compression) ---
                    momo_ok, momo_tag, momo_move, momo_accel, momo_v_now, momo_v_base, momo_taker, momo_basis, momo_liq_total = self._momo_impulse(sym, st, now)
                    best = float(getattr(st, "momo_best_abs", 0.0))
                    best_ts = float(getattr(st, "momo_best_ts", 0.0))
                    cur = abs(momo_move)

                    # если не улучшилось хотя бы на 0.6% за 3 минуты — не шлём снова
                    if cur < best + 0.6 and (now - best_ts) < 180:
                        continue

                    st.momo_best_abs = cur
                    st.momo_best_ts = now

                    if momo_ok:
                        p24h = float(getattr(st, "p24h", 0.0))

                        momo_taker = cached_latest(getattr(st, "taker_ratio", None), default=None)
                        momo_basis = cached_latest(getattr(st, "basis", None), default=None)

                        if momo_taker is None:
                            momo_taker = await self._get_taker_ratio(http, sym)
                        if momo_basis is None:
                            momo_basis = await self._get_basis_pct(http, sym)

                        momo_taker = float(momo_taker)
                        momo_basis = float(momo_basis)

                        # --- MOMO quality / manual-check hints ---
                        warn = []

                            # 1) alignment checks (если данные есть)
                        if momo_move > 0:
                            if momo_taker < 1.0:
                                warn.append("taker<1 (sell pressure)")
                            if momo_basis < -BASIS_MIN_ABS_PCT:
                                warn.append("basis<0 (fut weak)")
                        else:
                            if momo_taker > 1.0:
                                warn.append("taker>1 (buy pressure)")
                            if momo_basis > BASIS_MIN_ABS_PCT:
                                warn.append("basis>0 (fut strong)")

                        # 2) "late" check: откат от локального экстремума внутри окна
                        pts = ring_window_prices(st.price, MOMO_LOOKBACK_SEC, now)
                        vals = [v for _, v in pts] if pts else []
                        if len(vals) >= 8:
                            hi = max(vals); lo = min(vals); last = vals[-1]
                            if momo_move > 0 and hi > 0:
                                dd = (hi - last) / hi * 100.0
                                if dd >= 0.35:
                                    warn.append(f"pullback {dd:.2f}% (late)")
                            if momo_move < 0 and lo > 0:
                                bounce = (last - lo) / lo * 100.0
                                if bounce >= 0.35:
                                    warn.append(f"bounce {bounce:.2f}% (late)")

                        hint = "OK" if not warn else ("CHECK: " + ", ".join(warn))



                        msg = (
                            f"{sym} VERY_HOT {momo_tag}({ 'LONG' if momo_move>0 else 'SHORT' }) | "
                            f"move={momo_move:.2f}%/{MOMO_LOOKBACK_SEC}s | vol_accel={momo_accel:.2f}x | "
                            f"v_now={momo_v_now:.1f} v_base={momo_v_base:.1f} | "
                            f"liq={momo_liq_total/1000:.0f}K | taker={momo_taker:.2f} basis={momo_basis:+.3f}% | p24h={p24h:.2f}%"
                            f" | {hint}"
                        )
                        print(msg)

                        rank = 110.0 + clamp(abs(momo_move), 0.0, 30.0) + clamp(momo_accel, 0.0, 20.0)
                        # if conflicting/late -> don't spam TG unless it's VERY strong
                        send_to_tg = (not warn) or (abs(momo_move) >= 4.5 and momo_accel >= 10.0)

                        if send_to_tg:
                            signals.append((rank, msg))

                        continue

                    # --- remember peak after MOMO_UP (for FADE) ---
                    if momo_ok and momo_tag == "MOMO_UP":
                        st.momo_peak_price = prices[-1]
                        st.momo_peak_ts = now

                    if momo_ok:
                        last_ts = float(getattr(st, "momo_last_ts", 0.0))
                        if now - last_ts >= MOMO_COOLDOWN_SEC:
                            st.momo_last_ts = now


                    # --- FADE after pump (short) ---
                    # --- FADE after pump (short) ---
                    peak = float(getattr(st, "momo_peak_price", 0.0))
                    peak_ts = float(getattr(st, "momo_peak_ts", 0.0))

                    if peak > 0.0 and (now - peak_ts) <= 300 and len(prices) >= 2:
                        last = float(prices[-1])

                    # обновляем пик, если цена делает новый хай
                        if last > peak:
                            st.momo_peak_price = last
                            st.momo_peak_ts = now
                        else:
                        # откат от пика в %
                            retr = (peak - last) / peak * 100.0

                            taker = float(cached_latest(getattr(st, "taker_ratio", None), default=1.0))
                            basis = float(cached_latest(getattr(st, "basis", None), default=0.0))

                            if retr >= 1.5 and taker <= 0.75 and basis < 0:
                                msg = f"{sym} VERY_HOT FADE_SHORT | retr={retr:.2f}% | taker={taker:.2f} basis={basis:+.3f}%"
                                print(msg)
                                signals.append((120.0 + retr, msg))
                                st.momo_peak_price = 0.0  # чтобы не спамило




                    # добавляем в кандидаты, чтобы дальше OI/funding могли подтянуться
                            self.book.upsert(sym, 95.0 + clamp(abs(momo_move), 0.0, 20.0))


                    ok_price, rng = price_compression(prices)
                    if not ok_price:
                        self.stats.no_compress += 1
                        continue

                    rng_pct = rng * 100.0
                    if rng_pct > MAX_RANGE_SETUP_PCT:
                        self.stats.no_compress += 1
                        continue

                    fast_move = abs(_pct_move(prices[0], prices[-1]))
                    if fast_move > MAX_FAST_MOVE_SETUP_PCT:
                        self.stats.fast_move += 1
                        continue

                    ok_vol, accel, v_now, v_base = volume_acceleration_time(
                        st.volume,
                        short_sec=VOL_SHORT_SEC,
                        long_sec=VOL_LONG_SEC,
                        accel_thresh=VOL_ACCEL_THRESH,
                        min_now=VOL_MIN_NOW,
                        min_base=VOL_MIN_BASE,
                        accel_cap=VOL_ACCEL_CAP,
                    )
                    if not ok_vol:
                        self.stats.vol_fail += 1
                        continue

                    bias, move_pct, pos = direction_bias(prices)

                    if not INCLUDE_NEUTRAL and bias == "NEUTRAL":
                        self.stats.bias_filtered += 1
                        continue
                    if MODE == "LONG" and bias != "LONG":
                        self.stats.bias_filtered += 1
                        continue
                    if MODE == "SHORT" and bias != "SHORT":
                        self.stats.bias_filtered += 1
                        continue

                    p24h = float(getattr(st, "p24h", 0.0))
                    if bias == "LONG" and p24h > 8.0:
                        self.stats.p24h_hot += 1
                        continue
                    if bias == "SHORT" and p24h < -15.0:
                        self.stats.p24h_hot += 1
                        continue

                    setup_score = calc_setup_score(rng_pct=rng_pct, accel=accel, bias=bias, move_pct=move_pct, pos=pos)
                    if setup_score < BOOK_MIN_SCORE:
                        self.stats.score_low += 1
                        continue

                    if not hasattr(st, "setup_bucket"):
                        st.setup_bucket = 0
                    if not hasattr(st, "setup_last_ts"):
                        st.setup_last_ts = 0.0

                    bucket = int(setup_score // SETUP_BUCKET_STEP) * SETUP_BUCKET_STEP
                    if bucket <= st.setup_bucket and (now - st.setup_last_ts) < SETUP_COOLDOWN_SEC:
                        self.stats.setup_suppressed += 1
                        continue

                    st.setup_bucket = bucket
                    st.setup_last_ts = now
                    st.setup_score = setup_score

                    self.book.upsert(sym, setup_score)

                    # печатаем SETUP только если score достаточно высокий, и не слишком часто
                    if setup_score >= SETUP_PRINT_MIN_SCORE:
                        last_p = float(getattr(st, "setup_print_ts", 0.0))
                        best_p = float(getattr(st, "setup_print_best", 0.0))
                        if (now - last_p) >= SETUP_PRINT_COOLDOWN_SEC or (setup_score >= best_p + SETUP_PRINT_MIN_IMPROVE):
                            st.setup_print_ts = now
                            st.setup_print_best = max(best_p, float(setup_score))
                            self.stats.setup_printed += 1
                            print(
                                f"{sym} SETUP({bias}) | range={rng_pct:.3f}% | "
                                f"vol_accel={accel:.2f}x | v_now={v_now:.1f} v_base={v_base:.1f} | "
                                f"q24h={fmt_q24h(float(q24h))} | p24h={p24h:.2f}% | score={setup_score:.1f} | "
                                f"trend={move_pct:.2f}% pos={pos:.2f}"
                            )
                        else:
                            self.stats.setup_suppressed += 1

                # ========== STAGE 2 + 3: CONFIRM -> BREAKOUT_HOLD ==========
                for sym in self.book.top():
                    st = STATES.get(sym)
                    if st is None:
                        continue

                    self.stats.candidates += 1

                    if sym in BLOCKLIST or sym in STABLE_LIKE:
                        continue

                    prices = ring_values(st.price)
                    if len(prices) < SETUP_MIN_PRICE_POINTS:
                        self.stats.s2_few_prices += 1
                        continue

                    momo_ok, momo_tag, momo_move, momo_accel, momo_v_now, momo_v_base, momo_taker, momo_basis, momo_liq_total = self._momo_impulse(sym, st, now)

                    if momo_ok:
                        level = "VERY_HOT"
                        tag_out = momo_tag
                    # собери msg в том же формате что остальные (добавь momo_* поля)
                    # и дальше пусть идёт в твой signals.append(...) + should_send_tg(...)

                    


                    ok_price, rng = price_compression(prices)
                    if not ok_price:
                        self.stats.s2_no_compress += 1
                        continue

                    rng_pct = rng * 100.0
                    if rng_pct > CONFIRM_MAX_RANGE_PCT:
                        self.stats.range_big += 1
                        continue

                    ok_vol, accel, v_now, v_base = volume_acceleration_time(
                        st.volume,
                        short_sec=VOL_SHORT_SEC,
                        long_sec=VOL_LONG_SEC,
                        accel_thresh=VOL_ACCEL_THRESH,
                        min_now=VOL_MIN_NOW,
                        min_base=VOL_MIN_BASE,
                        accel_cap=VOL_ACCEL_CAP,
                    )
                    if not ok_vol:
                        self.stats.s2_vol_fail += 1
                        continue

                    bias, move_pct, pos = direction_bias(prices)
                    if not INCLUDE_NEUTRAL and bias == "NEUTRAL":
                        continue
                    if MODE == "LONG" and bias != "LONG":
                        continue
                    if MODE == "SHORT" and bias != "SHORT":
                        continue

                    p24h = float(getattr(st, "p24h", 0.0))
                    if bias == "LONG" and p24h > 8.0:
                        self.stats.s2_p24h_hot += 1
                        continue
                    if bias == "SHORT" and p24h < -15.0:
                        self.stats.s2_p24h_hot += 1
                        continue

                    if len(ring_values(st.oi)) < CONFIRM_MIN_OI_POINTS:
                        self.stats.few_oi += 1
                        continue

                    oi_pct = oi_pct_change_recent(st.oi, sec=60)

                    level = "WATCH"
                    if oi_pct >= 6.0 or accel >= 4.0:
                        level = "HOT"
                    if oi_pct >= 10.0 or accel >= 6.0:
                        level = "VERY_HOT"

                    if oi_pct < 1.0 and level in ("HOT", "VERY_HOT"):
                        level = "WATCH"
                        self.stats.oi_not_growing += 1

                    last_price = prices[-1]
                    if bias == "LONG":
                        brk_now, brk_level = breakout_level_up(prices, lookback=BREAKOUT_LOOKBACK, pad_pct=BREAKOUT_PAD_PCT)
                    else:
                        brk_now, brk_level = breakout_level_down(prices, lookback=BREAKOUT_LOOKBACK, pad_pct=BREAKOUT_PAD_PCT)

                    brk_hold_ok, brk_age = self._breakout_hold_ok(st, bias, brk_now, brk_level, last_price, now)

                    spike, sp_move, sp_vr, sp_vol = spike_detector(
                        price_ring=st.price,
                        vol_ring=st.volume,
                        side=bias,
                        short_sec=SPIKE_SHORT_SEC,
                        long_sec=VOL_LONG_SEC,
                        min_move_pct=SPIKE_MIN_MOVE_PCT,
                        min_vol_ratio=SPIKE_MIN_VOL_RATIO,
                        min_vol_usdt=SPIKE_MIN_VOL_USDT,
                    )
                    if not spike:
                        sp_move = 0.0
                        sp_vr = 0.0
                        sp_vol = 0.0

                    fund_delta, fund_now, fund_flip = funding_delta_recent(st.funding, sec=FUNDING_LOOKBACK_SEC)
                    fund_ok = (abs(fund_delta) >= FUNDING_DELTA_MIN) or fund_flip
                    fund_dir_ok = (fund_delta > 0) if bias == "LONG" else (fund_delta < 0)

                    liq_buy, liq_sell, liq_total, liq_imb = self._liq_window_stats(st, LIQ_WINDOW_SEC, now=now)
                    liq_dir_ok = (liq_imb >= LIQ_IMB_MIN) if bias == "LONG" else (liq_imb <= -LIQ_IMB_MIN)
                    taker_ratio = cached_latest(getattr(st, "taker_ratio", None)) or cached_latest(getattr(st, "taker", None))
                    basis_pct = cached_latest(getattr(st, "basis_pct", None)) or cached_latest(getattr(st, "basis", None))

                    tag_out = "PRE"
                    force_print = False

                    pre_squeeze = spike and (oi_pct >= OI_MIN_PCT_FOR_SQUEEZE) and fund_ok and fund_dir_ok
                    liq_sweep = (liq_total >= LIQ_MIN_USDT) and liq_dir_ok and (brk_hold_ok or spike)

                    # If we might promote to SQUEEZE or we will print, ensure taker/basis are available (REST fallback)
                    if pre_squeeze:
                        if USE_TAKER_RATIO and taker_ratio is None:
                            taker_ratio = await self._get_taker_ratio(http, sym)
                        if USE_BASIS and basis_pct is None:
                            basis_pct = await self._get_basis_pct(http, sym)
                    if (brk_hold_ok or liq_sweep) and USE_BASIS and basis_pct is None:
                        basis_pct = await self._get_basis_pct(http, sym)
                    if (brk_hold_ok or liq_sweep) and USE_TAKER_RATIO and taker_ratio is None:
                        taker_ratio = await self._get_taker_ratio(http, sym)

                    basis_ok = (basis_pct is not None) and (abs(basis_pct) >= BASIS_MIN_ABS_PCT) if USE_BASIS else True
                    basis_dir_ok = (basis_pct is not None) and ((basis_pct > 0) if bias == "LONG" else (basis_pct < 0)) if USE_BASIS else True
                    taker_dir_ok = (taker_ratio is not None) and ((taker_ratio >= 1.05) if bias == "LONG" else (taker_ratio <= 0.95)) if USE_TAKER_RATIO else True

                    squeeze = pre_squeeze and basis_ok and basis_dir_ok and taker_dir_ok

                    if squeeze:
                        level = "VERY_HOT"
                        tag_out = "SQUEEZE_UP" if bias == "LONG" else "SQUEEZE_DOWN"
                        force_print = True

                    elif liq_sweep and basis_ok and (basis_dir_ok or not USE_BASIS):
                        level = "VERY_HOT"
                        tag_out = "LIQ_SWEEP_UP" if bias == "LONG" else "LIQ_SWEEP_DOWN"
                        force_print = True

                    elif brk_hold_ok and (taker_dir_ok or oi_pct >= 2.0):
                        tag_out = "BREAKOUT_UP" if bias == "LONG" else "BREAKOUT_DOWN"
                        force_print = True

                    else:
                        continue

                    want_print = force_print and (PRINT_BREAKOUT_WATCH or level in ("HOT", "VERY_HOT"))
                    if not want_print:
                        continue

                    last_level = getattr(st, "last_alert_level", "IGNORE")
                    last_ts = float(getattr(st, "last_alert_ts", 0.0))
                    upgraded = LEVEL_RANK[level] > LEVEL_RANK.get(last_level, 0)
                    cd = ALERT_COOLDOWN.get(level, 180)

                    if (not upgraded) and (now - last_ts < cd):
                        self.stats.confirm_suppressed += 1
                        continue

                    st.last_alert_level = level
                    st.last_alert_ts = now
                    self.stats.confirm_printed += 1

                    setup_score = float(getattr(st, "setup_score", self.book.get_score(sym)))

                    msg = (
                        f"{sym} {level} {tag_out}({bias}) | range={rng_pct:.3f}% | "
                        f"vol_accel={accel:.2f}x | oi+={oi_pct:.1f}% | p24h={p24h:.2f}% | "
                        f"score={setup_score:.1f} | "
                        f"fund={fmt_funding(fund_now)} dFund={fmt_funding(fund_delta)} | "
                        f"spike={sp_move:.2f}% vr={sp_vr:.2f}x | "
                        f"liq={liq_total/1000:.0f}K imb={liq_imb:+.2f} | "
                        f"taker={fmt_float_opt(taker_ratio,2)} basis={fmt_signed_pct_opt(basis_pct,3)} | "
                        f"trend={move_pct:.2f}% pos={pos:.2f}"
                    )
                    print(msg)

                    if self._should_send_tg(level, tag=tag_out, oi_pct=oi_pct, sp_vr=sp_vr, liq_total=liq_total):
                        rank = setup_score
                        if tag_out.startswith("MOMO"):
                            rank += 25.0
                        if tag_out.startswith("SQUEEZE"):
                            rank += 30.0
                        if tag_out.startswith("LIQ"):
                            rank += 20.0
                        if brk_hold_ok:
                            rank += 10.0
                        rank += clamp(abs(oi_pct), 0.0, 15.0)
                        signals.append((rank, msg))

                if self.tg_enabled and signals:
                    signals.sort(key=lambda x: x[0], reverse=True)
                    for _, msg in signals[:TG_SEND_TOP_N]:
                        if self.tg_rl.allow():
                            try:
                                await _tg_send(http, self.tg_token, self.tg_chat_id, msg)
                            except Exception:
                                pass
