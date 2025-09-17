from typing import Generator
from uuid import uuid4
import random


def get_many(n: int = 100) -> Generator[dict, dict, dict]:
    for _ in range(n):
        yield {
            "v1": random.randint(1, 10),
            "k2": round(random.uniform(1, 1000), 1),
            "my_string": str(uuid4()),
        }
