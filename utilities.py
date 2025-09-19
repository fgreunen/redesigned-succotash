from functools import wraps
from time import time
from typing import Generator
from uuid import uuid4
import random


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print(f"Func: [{f.__name__}] took: {(te - ts):.4f} sec.")
        return result

    return wrap


def get_many(n: int = 100) -> Generator[dict, dict, dict]:
    for _ in range(n):
        yield {
            "v1": random.randint(1, 10),
            "k2": round(random.uniform(1, 1000), 1),
            "my_string": str(uuid4()),
            # "const": "constant String",  # TODO: enable this.
        }
