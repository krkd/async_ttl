import asyncio
from collections import defaultdict
from itertools import chain
from threading import Lock
from typing import Dict, Generic, Optional, Set, Tuple, TypeVar, cast

__version__ = '0.0.4'

__all__ = ('AsyncTTL',)

KT = TypeVar('KT')
VT = TypeVar('VT')
Storage_VT = Tuple[float, int, VT]


sentinel = object()


class AsyncTTL(Generic[KT, VT]):
    def __init__(
        self,
        *,
        resolution: int = 60,
        loop: Optional[asyncio.events.AbstractEventLoop] = None
    ):
        if loop is None:
            loop = asyncio.get_event_loop()

        self._storage: Dict[KT, Storage_VT] = {}
        self._expire_buckets: Dict[int, Set[KT]] = defaultdict(set)
        self._resolution = int(resolution)
        self._lock = Lock()

        self._loop = loop
        self._handle = self._loop.call_later(self._resolution, self._cleanup)

    def get(self, key: KT):
        with self._lock:
            return self._get(key)

    def _get(self, key: KT, now: Optional[float] = None) -> VT:
        if now is None:
            now = self._loop.time()

        expire, _, value = self._storage[key]
        if expire < now:
            self._storage.pop(key, None)
            raise KeyError
        return value

    def setex(self, key: KT, expire: float, value: VT):
        with self._lock:
            self._setex(key, expire, value)

    def _setex(
        self,
        key: KT,
        expire: float,
        value: VT,
        now: Optional[float] = None,
    ):
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

    def ttl(self, key: KT) -> float:
        stored = self._storage.get(key, sentinel)
        if stored is sentinel:
            return -2

        # TODO: figure out why type inheritance does not work
        stored = cast(Storage_VT, stored)

        now = self._loop.time()
        ttl = stored[0] - now
        if ttl < 0:
            return -2

        return ttl

    def expire(self, key: KT, expire: float):
        with self._lock:
            now = self._loop.time()

            value = self._get(key, now=now)
            self._setex(key, expire, value, now=now)

    def remove(self, key: KT):
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
