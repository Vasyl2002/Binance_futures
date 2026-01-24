# buffer.py
from collections import deque
import time

class Ring:
    """
    Хранит точки (ts, value).
    Совместим с тем, что тебе нужно: .add(), .values(), .window(), .span_sec(), .d
    """
    def __init__(self, maxlen: int = 2048):
        self.d = deque(maxlen=maxlen)

    def add(self, value: float, ts: float | None = None):
        if ts is None:
            ts = time.time()
        self.d.append((float(ts), float(value)))

    def values(self):
        return [v for _, v in self.d]

    def items(self):
        return list(self.d)

    def span_sec(self) -> float:
        if len(self.d) < 2:
            return 0.0
        return float(self.d[-1][0] - self.d[0][0])

    def window(self, sec: int, now: float | None = None):
        if now is None:
            now = time.time()
        cutoff = now - float(sec)
        # вернём (ts, val) за окно
        return [(t, v) for (t, v) in self.d if t >= cutoff]





def points(self):
    """
    Возвращает список (ts, value).
    Подстрой под свою реализацию, если хранение другое.
    """
    if hasattr(self, "_ts") and hasattr(self, "_vals"):
        return list(zip(self._ts, self._vals))
    if hasattr(self, "ts") and hasattr(self, "vals"):
        return list(zip(self.ts, self.vals))
    # если у тебя хранится list[tuple] уже:
    if hasattr(self, "_data") and self._data and isinstance(self._data[0], tuple):
        return list(self._data)
    raise AttributeError("Ring.points(): can't find internal storage. Adapt points() to your Ring internals.")

def window(self, sec: float, now: float | None = None):
    """
    Возвращает точки (ts, val) за последние sec секунд.
    """
    import time
    now = time.time() if now is None else now
    pts = self.points()
    cutoff = now - sec
    return [(t, v) for (t, v) in pts if t >= cutoff]



