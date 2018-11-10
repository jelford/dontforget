"""Cache the results of expensive functions"""

__version__ = "0.0.1"

from pathlib import Path
from os import makedirs
from functools import wraps
from hashlib import blake2b
import sqlite3
import pickle
import gzip
import json
from typing import Callable, Tuple, Optional, Any


__all__ = ["set_hash_customization", "set_storage_directory", "cached"]


def set_hash_customization(custom_hash_data: bytes) -> None:
    """
    Before using any hashed function, you may personalize the hash key used by
    dontforget with data of your own. Use this to bust the cache, for example
    between deployments of your software, enabling parallel tests to run against
    the same cache, or when the internals of your classes have changed in such
    a way as to make unpickling unsafe.

    :param custom_hash_data: up to 16 bytes of data which will be used to personalize
    the hash function.
    """
    global _custom_hash_data
    if len(custom_hash_data) > 16:
        raise ValueError(
            "custom_hash_data too large to be used for hash personalization"
        )

    _custom_hash_data = custom_hash_data


def set_storage_directory(new_cache_root: Path) -> None:
    """
    :param new_cache_root: choose a new storage location for dontforget's data.
    """
    global _cache_root
    _cache_root = Path(new_cache_root)


def cached(func: Callable) -> Callable:
    """
    Function decorator that caches the results of invocations to the local file system.

    :param func: The function to cache must take only hashable arguments, 
    or dictionaries with hashable keys and values. The function's output
    must be pickleable.
    """
    global _CACHED_ABSENT_MARKER
    makedirs(_cache_root, exist_ok=True)

    @wraps(func)
    def cached_func(*args, **kwargs):
        key = _cache_key_from(func, *args, **kwargs)

        cached_value = _lookup_in_cache(key)

        if cached_value is _CACHED_ABSENT_MARKER:
            return None
        elif cached_value is not None:
            return cached_value

        loaded_value = func(*args, **kwargs)

        _put_in_cache(key, loaded_value)
        return loaded_value

    return cached_func


_cache_root = Path.cwd() / ".dontforget-cache"
_CACHED_ABSENT_MARKER = object()


class UnrecognizedCacheEncodingException(RuntimeError):
    pass


_custom_hash_data = b"dontforget" + __version__.encode("utf-8")


def _cache_key_from(func, *args, **kwargs) -> str:
    h = blake2b(digest_size=32, person=_custom_hash_data)

    h.update(func.__name__.encode("utf-8"))
    h.update(func.__code__.co_code)
    h.update(str(func.__code__.co_consts).encode("utf-8"))
    h.update(str(func.__defaults__).encode("utf-8"))
    h.update(str(func.__kwdefaults__).encode("utf-8"))

    for a in args:
        a_as_bytes = f"{hash(a)}".encode("utf-8")
        h.update(a_as_bytes)

    for k, v in kwargs.items():
        k_as_bytes = f"{hash(k)}".encode("utf-8")
        h.update(k_as_bytes)
        v_as_bytes = f"{hash(v)}".encode("utf-8")
        h.update(v_as_bytes)

    return f"{func.__name__}-{h.hexdigest()}"


def _encode(data) -> Tuple[Optional[bytes], Optional[str]]:
    if data is None:
        return None, None
    if type(data) == str:
        return data.encode("utf-8"), "str/utf-8"
    if type(data) in (dict, list):
        try:
            return json.dumps(data).encode("utf-8"), "json/utf-8"
        except ValueError:
            pass
    return pickle.dumps(data), "pickle"


def _decode(data, format) -> Any:
    if format == "str/utf-8":
        return data.decode("utf-8")
    if format == "json/utf-8":
        return json.loads(data.decode("utf-8"))
    if format == "pickle":
        return pickle.loads(data)
    else:
        raise UnrecognizedCacheEncodingException(format)


def _put_in_cache(key, value) -> None:

    if value is not None:
        encoded_data, data_format = _encode(value)
        data_to_cache: Optional[bytes] = gzip.compress(encoded_data, 9)
        assert data_to_cache is not None  # for mypy
        absent_marker = 0
        if len(data_to_cache) < 4000:
            contents_for_db: Optional[bytes] = data_to_cache
            path_to_file = None
        else:
            contents_for_db = None
            path_to_file = f"{key}.gz"
            with open(_cache_root / path_to_file, "wb") as f:
                f.write(data_to_cache)
    else:
        data_format = None
        data_to_cache = None
        contents_for_db = None
        path_to_file = None
        absent_marker = 1

    with sqlite3.connect(str(_cache_root / "index.db")) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS objects """
            """(func_hash TEXT PRIMARY KEY, path TEXT, content BLOB, format TEXT, absent INT)"""
        )
        conn.execute(
            """INSERT INTO objects VALUES (?, ?, ?, ?, ?)""",
            (key, path_to_file, contents_for_db, data_format, absent_marker),
        )


def _lookup_in_cache(key) -> Any:
    with sqlite3.connect(str(_cache_root / "index.db")) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """SELECT path, content, format, absent FROM objects WHERE func_hash = ?""",
                (key,),
            )
        except sqlite3.OperationalError:
            return None
        result = cursor.fetchone()

    if result is None:
        return

    path, content, format, absent = result

    if absent == 1:
        return _CACHED_ABSENT_MARKER

    if content is None:
        assert path, "Found entry in db without content but also without path"
        with open(path, "rb") as content_f:
            content = content_f.read()

    decompressed = gzip.decompress(content)
    decoded = _decode(decompressed, format)
    return decoded
