from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print(f"Func: [{f.__name__}] took: {(te - ts):.4f} sec.")
        return result

    return wrap
