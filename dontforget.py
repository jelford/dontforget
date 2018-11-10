"""Cache the results of expensive functions"""

__version__ = '0.0.1'

from pathlib import Path
from os import makedirs
from functools import wraps 
from hashlib import blake2b
import sqlite3
import pickle
import gzip
import json


_CACHED_ABSENT_MARKER = object()
_cache_root = Path.cwd() / '.dontforget-cache'

class UnrecognizedCacheEncodingException(RuntimeError):
    pass


_custom_hash_data = b'dontforget'+__version__.encode('utf-8')

def set_hash_customization(custom_hash_data: bytes):
    """
    
    """
    global _custom_hash_data
    if len(custom_hash_data > 16):
        raise ValueError('custom_hash_data too large to be used for hash personalization')
    
    _custom_hash_data = custom_hash_data


def set_storage_directory(new_cache_root: Path):
    global _cache_root
    _cache_root = Path(new_cache_root)

def _cache_key_from(func, *args, **kwargs):
    h = blake2b(digest_size=32, person=_custom_hash_data)
    print(f'Customizing hash: {_custom_hash_data}')
    
    h.update(func.__name__.encode('utf-8'))
    h.update(func.__code__.co_code)

    for a in args:
        a_as_bytes = f'{a}'.encode('utf-8')
        h.update(a_as_bytes)

    for k, v in kwargs.items():
        k_as_bytes = f'{k}'.encode('utf-8')
        h.update(k_as_bytes)
        v_as_bytes = f'{_cache_key_from(v)}'.encode('utf-8')
        h.update(v_as_bytes)
    
    return f'{func.__name__}-{h.hexdigest()}'


def _encode(data):
    if data is None:
        return None, None
    if type(data) == str:
        return data.encode('utf-8'), 'str/utf-8'
    if type(data) in (dict, list):
        try:
            return json.dumps(data).encode('utf-8'), 'json/utf-8'
        except ValueError:
            pass
    return pickle.dumps(data), 'pickle'


def _decode(data, format):
    if format == 'str/utf-8':
        return data.decode('utf-8')
    if format == 'json/utf-8':
        return json.loads(data.decode('utf-8'))
    if format == 'pickle':
        return pickle.loads(data)
    else:
        raise UnrecognizedCacheEncodingException(format)
    

def _put_in_cache(key, value):

    if value is not None:
        encoded_data, data_format = _encode(value)
        data_to_cache = gzip.compress(encoded_data, 9)
        absent_marker = 0
        if len(data_to_cache) < 4000:
            contents_for_db = data_to_cache
            path_to_file = None
        else:
            contents_for_db = None
            path_to_file = f'{key}.gz'
            with open(_cache_root / path_to_file, 'wb') as f:
                f.write(data_to_cache)
    else:
        data_format = None
        data_to_cache = None
        contents_for_db = None
        path_to_file = None
        absent_marker = 1

    with sqlite3.connect(str(_cache_root / 'index.db')) as conn:
        conn.execute(
            '''CREATE TABLE IF NOT EXISTS objects '''
            '''(func_hash TEXT PRIMARY KEY, path TEXT, content BLOB, format TEXT, absent INT)''')
        conn.execute(
            '''INSERT INTO objects VALUES (?, ?, ?, ?, ?)''', 
            (key, path_to_file, contents_for_db, data_format, absent_marker)
        )


def _lookup_in_cache(key):
    with sqlite3.connect(str(_cache_root / 'index.db')) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                '''SELECT path, content, format, absent FROM objects WHERE func_hash = ?''', 
                (key,)
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
        with open(path, 'rb') as content_f:
            content = content_f.read()
    
    decompressed = gzip.decompress(content)
    decoded = _decode(decompressed, format)
    return decoded


def cached(func):
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

