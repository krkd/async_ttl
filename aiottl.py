import asyncio
import time
from collections import defaultdict
from itertools import chain
from threading import Lock


class Aiottl:
    def __init__(self, *, loop, resolution=60):
        self._storage = {}
        self._expire_buckets = defaultdict(set)
        self._resolution = int(resolution)
        self._lock = Lock()

        self._loop = loop
        self._handle = self._loop.call_later(self._resolution, self._cleanup)

    def get(self, key):
        with self._lock:
            return self._get(key)

    def _get(self, key, now=None):
        now = self._loop.time() if now is None else now
        expire, value = self._storage[key]
        if expire < now:
            self._storage.pop(key, None)
            raise KeyError
        return value

    def setex(self, key, expire, value):
        with self._lock:
            self._setex(key, expire, value)

    def _setex(self, key, expire, value, now=None):
        expire = int(expire)

        if expire <= 0:
            raise ValueError('Expire should be greater than 0')

        now = self._loop.time() if now is None else now

        expire_at = now + expire
        bucket_key = int(expire_at // self._resolution)

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

        now = self._loop.time()
        ttl = stored[0] - now
        if ttl < 0:
            return -2

        return ttl
    
    def expire(self, key, expire):
        with self._lock:
            now = self._loop.time()

            value = self._get(key, now=now)
            self._setex(key, expire, value, now=now)

    def remove(self, key):
        self._storage.pop(key, None)

    def _cleanup(self):
        with self._lock:
            now = self._loop.time()
            current_bucket = int(now // self._resolution)

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

    def __del__(self):
        self._handle.cancel()
