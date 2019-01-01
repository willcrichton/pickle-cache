import os
import pickle
import gc
import marshal
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import math
from enum import Enum
import multiprocessing as mp
import itertools


# https://mathieularose.com/how-not-to-flatten-a-list-of-lists-in-python/
def flatten(l):
    return list(itertools.chain.from_iterable(l))


def par_for(f, l, process=False, workers=None, progress=True):
    Pool = ProcessPoolExecutor if process else ThreadPoolExecutor
    with Pool(max_workers=mp.cpu_count()
              if workers is None else workers) as executor:
        if progress:
            return list(tqdm(executor.map(f, l), total=len(l), smoothing=0.05))
        else:
            return list(executor.map(f, l))


class CacheMethod(Enum):
    Pickle = 1
    Marshal = 2
    Numpy = 3


class PickleCache:
    def __init__(self,
                 cache_dir=None,
                 default_method=CacheMethod.Pickle,
                 default_num_chunks=8):
        if cache_dir is None:
            cache_dir = os.path.expanduser('~/.picklecache')
        self._cache_dir = cache_dir
        os.makedirs(self._cache_dir, exist_ok=True)

        self._default_method = default_method
        self._default_num_chunks = default_num_chunks

    def _fname(self, k, i, method):
        exts = {
            CacheMethod.Pickle: 'pkl',
            CacheMethod.Marshal: 'msl',
            CacheMethod.Numpy: 'bin'
        }
        return '{}/{}_{}.{}'.format(self._cache_dir, k, i, exts[method])

    def has(self, k, i=0, method=None):
        return os.path.isfile(
            self._fname(k, i, method
                        if method is not None else self._default_method))

    def set(self, k, v, method=None, chunks=None):
        method = method if method is not None else self._default_method

        def save_chunk(args):
            (i, v) = args
            with open(self._fname(k, i, method), 'wb') as f:
                if method == CacheMethod.Marshal:
                    marshal.dump(v, f)
                elif method == CacheMethod.Numpy:
                    for arr in v:
                        f.write(arr.tobytes())
                elif method == CacheMethod.Pickle:
                    pickler = pickle.Pickler(f, pickle.HIGHEST_PROTOCOL)
                    pickler.fast = 1  # https://stackoverflow.com/a/15108940/356915
                    pickler.dump(v)
                else:
                    raise Exception("Invalid cache method {}".format(method))

        num_chunks = chunks if chunks is not None else self._default_num_chunks

        gc.disable()  # https://stackoverflow.com/a/36699998/356915
        if (isinstance(v, list)
                or isinstance(v, tuple)) and len(v) >= num_chunks:
            n = len(v)
            chunk_size = int(math.ceil(float(n) / num_chunks))
            par_for(
                save_chunk, [(i, v[(i * chunk_size):((i + 1) * chunk_size)])
                             for i in range(num_chunks)],
                progress=False,
                workers=1)
        else:
            save_chunk((0, v))
        gc.enable()

    def get(self, k, fn=None, force=False, method=None, chunks=None, **kwargs):
        method = method if method is not None else self._default_method

        if not (all([self.has(k2, 0, m2) for k2, m2 in zip(k, method)])
                if isinstance(k, tuple) else self.has(k, 0, method)) or force:
            if fn is not None:
                v = fn()
                if isinstance(k, tuple):
                    for (k2, v2, m2) in zip(k, v, method):
                        self.set(k2, v2, m2)
                else:
                    self.set(k, v, method)
                return v
            else:
                raise Exception('Missing cache key {}'.format(k))

        num_chunks = chunks if chunks is not None else self._default_num_chunks

        if isinstance(k, tuple):
            return tuple([
                self.get(k2, method=m2, **kwargs) for k2, m2 in zip(k, method)
            ])

        else:

            def load_chunk(i):
                with open(self._fname(k, i, method), 'rb') as f:
                    if method == CacheMethod.Marshal:
                        return marshal.load(f)
                    elif method == CacheMethod.Numpy:
                        dtype = kwargs['dtype']
                        size = np.dtype(dtype).itemsize * kwargs['length']
                        byte_str = f.read()
                        assert len(byte_str) % size == 0
                        return [
                            np.frombuffer(byte_str[i:i + size], dtype=dtype)
                            for i in range(0, len(byte_str), size)
                        ]
                    elif method == CacheMethod.Pickle:
                        return pickle.load(f, encoding='latin1')
                    else:
                        raise Exception(
                            "Invalid cache method {}".format(method))

            gc.disable()
            if self.has(k, 1, method):
                loaded = flatten(
                    par_for(
                        load_chunk,
                        list(range(num_chunks)),
                        workers=num_chunks,
                        progress=False))
            else:
                loaded = load_chunk(0)
            gc.enable()

            return loaded
