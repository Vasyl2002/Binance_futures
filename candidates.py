# candidates.py
import time

MAX_CANDIDATES = 20
TTL_SEC = 45 * 60  # держим кандидата 45 минут

_CAND = {}  # sym -> {"score": float, "ts": float}

def upsert(sym: str, score: float):
    now = time.time()
    _CAND[sym] = {"score": float(score), "ts": now}
    _prune(now)

def touch(sym: str):
    if sym in _CAND:
        _CAND[sym]["ts"] = time.time()

def remove(sym: str):
    _CAND.pop(sym, None)

def list_sorted() -> list[str]:
    _prune(time.time())
    return [k for k, _ in sorted(_CAND.items(), key=lambda kv: kv[1]["score"], reverse=True)]

def count() -> int:
    _prune(time.time())
    return len(_CAND)

def _prune(now: float):
    # TTL
    for s in list(_CAND.keys()):
        if now - _CAND[s]["ts"] > TTL_SEC:
            del _CAND[s]

    # top N
    if len(_CAND) > MAX_CANDIDATES:
        items = sorted(_CAND.items(), key=lambda kv: kv[1]["score"], reverse=True)[:MAX_CANDIDATES]
        keep = {k: v for k, v in items}
        _CAND.clear()
        _CAND.update(keep)
