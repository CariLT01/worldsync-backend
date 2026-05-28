import time
import threading


class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        """
        capacity: maximum tokens in bucket
        refill_rate: tokens added per second
        """
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)

        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill

        # add tokens based on time passed
        added = elapsed * self.refill_rate
        if added > 0:
            self._tokens = min(self.capacity, self._tokens + added)
            self._last_refill = now

    def allow_request(self, tokens: int = 1) -> bool:
        """
        Returns True if request is allowed, False otherwise.
        """
        with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            return False

    def get_tokens(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens