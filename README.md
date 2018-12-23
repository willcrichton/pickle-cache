# pickle-cache

Small utility for easily and efficiently saving/loading Python values to disk.

## Installation

```
pip install pickle-cache
```

## Usage

```python
from pickle_cache import PickleCache

pc = PickleCache()

pc.set('foo', 'bar')
assert(pc.get('foo') == 'bar')

class Test:
    def __init__(self, x):
        self._x = x

def make_test():
    return Test(1)

assert(pc.get('test', make_test)._x == 1) # compute if not exists
assert(pc.get('test', make_test)._x == 1) # cache if exists
```
