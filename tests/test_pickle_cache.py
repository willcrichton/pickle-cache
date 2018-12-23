from pickle_cache import PickleCache, CacheMethod
import pytest
import tempfile
import numpy as np

@pytest.fixture(scope='module')
def pc():
    # with tempfile.TemporaryDirectory() as dirname:
    dirname = tempfile.TemporaryDirectory().__enter__()
    print(dirname)
    pc = PickleCache(cache_dir=dirname)
    yield pc


def test_getset(pc):
    pc.set('foo', 'bar')
    assert(pc.get('foo') == 'bar')


def test_getfunction(pc):
    executed = 0
    def load():
        nonlocal executed
        executed += 1
        return executed
    pc.get('test_getfunction', load)
    assert pc.get('test_getfunction', load) == 1
    assert pc.get('test_getfunction', load, force=True) == 2


def test_marshal(pc):
    pc.set('foo', 'bar', method=CacheMethod.Marshal)
    assert(pc.get('foo', method=CacheMethod.Marshal) == 'bar')


def test_chunks(pc):
    l = list(range(100000))
    pc.set('test_chunks', l, chunks=13)
    assert(pc.get('test_chunks', chunks=13) == l)


def test_numpy(pc):
    l = np.array(list(range(100000)))
    pc.set('test_numpy', l, method=CacheMethod.Numpy)
    assert((pc.get('test_numpy', method=CacheMethod.Numpy, dtype=l.dtype, length=100000) == l).all())
