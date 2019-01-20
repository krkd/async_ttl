import asyncio
from collections import defaultdict
from itertools import chain
from threading import Lock

__version__ = '0.0.1'

__all__ = ('Aiottl',)


class Aiottl:
    def __init__(self, *, resolution=60, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

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
        if now is None:
            now = self._loop.time()

        expire, _, value = self._storage[key]
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

        if now is None:
            now = self._loop.time()

        expire_at = now + expire
        bucket_key = int(expire_at // self._resolution)

        if key in self._storage:
            old_expire, old_bucket_key, old_value = self._storage[key]
            self._expire_buckets[old_bucket_key].remove(key)

        self._expire_buckets[bucket_key].add(key)
        self._storage[key] = (expire_at, bucket_key, value)

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

            expired_keys = chain(
                self._expire_buckets[bucket] for bucket in expired
            )

            for key in expired_keys:
                self._storage.pop(key, None)

            for bucket in expired:
                self._expire_buckets.pop(bucket, None)

        if not self._loop.is_closed:
            self._handle = self._loop.call_later(
                self._resolution,
                self._cleanup,
            )

    def __del__(self):
        self._handle.cancel()
