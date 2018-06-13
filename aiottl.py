import asyncio
import time
from collections import defaultdict
from itertools import chain
from threading import RLock


class Aiottl:
    def __init__(self, *, loop, resolution=60):
        self._storage = {}
        self._expire_buckets = defaultdict(set)
        self._resolution = resolution
        self._lock = RLock()

        self._loop = loop
        self._handle = self._loop.call_later(self._resolution, self._cleanup)

    def get(self, key):
        now = time.monotonic()
        with self._lock:
            expire, value = self._storage[key]
            if expire < now:
                self._storage.pop(key, None)
                raise KeyError
        return value

    def setex(self, key, expire, value):
        assert expire > 0, 'expire should be positive integer'

        now = time.monotonic()

        expire_at = now + expire
        bucket_key = int(expire_at // self._resolution)

        with self._lock:
            if key in self._storage:
                old_expire, old_value = self._storage[key]
                old_bucket_key = int(now + old_expire // self._resolution)
                self._expire_buckets[old_bucket_key].remove(key)

            self._expire_buckets[bucket_key].add(key)
            self._storage[key] = (expire_at, value)

    def ttl(self, key):
        stored = self._storage.get(key)
        if stored is None:
            return -2

        now = time.monotonic()
        ttl = stored[0] - now
        if ttl < 0:
            return -2

        return ttl
    
    def expire(self, key, expire):
        with self._lock:
            self.setex(key, expire, self.get(key))

    def _cleanup(self):
        now = time.monotonic()
        current_bucket = int(now // self._resolution)

        with self._lock:
            expired = [
                bucket for bucket in self._expire_buckets.keys()
                if bucket < current_bucket
            ]

            for key in chain(self._expire_buckets[bucket] for bucket in expired):
                self._storage.pop(key, None)

            for bucket in expired:
                self._expire_buckets.pop(bucket, None)

        if not self._loop.is_closed:
            self._handle = self._loop.call_later(self._resolution, self._cleanup)

    def close(self):
        self._handle.cancel()
