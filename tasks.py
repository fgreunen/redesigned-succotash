import csv
import math
from invoke import task
from config import PATH_DATA
import psutil
from utilities import get_many, timing

from db import (
    copy_to_parquet,
    get_average_duckdb,
    get_average_bare,
    get_average_pandas,
    get_average_polars,
    init as db_init,
)

process = psutil.Process()


@task
def generate(_):
    db_init()
    N = 500000  # TODO: Make this 5m, and assess the change(-s).

    @timing
    def persist():
        with open(
            PATH_DATA,
            "w",
            newline="",
        ) as csv_file:
            writer = csv.writer(csv_file)
            l = list(get_many(N))
            for i, item in enumerate(l):
                if i == 0:
                    writer.writerow(list(item.keys()))
                writer.writerow(list(item.values()))
                if i % 250000 == 0:
                    current_mem_usage_mb = math.ceil(
                        process.memory_info().rss / (1024 * 1024)
                    )
                    # TODO: Why is memory usage so high?
                    # TODO: Why is memory usage constant?
                    progress = math.ceil(100 * i / N)
                    print(f"Progress - {progress}% ({current_mem_usage_mb}MB used).")

    persist()
    copy_to_parquet()  # TODO: Interpret the size difference.
    # TODO: How to calculate and print the rate per second?


@task
def analyse(_):
    print(f"Current average (DuckDB) is: {get_average_duckdb()}.")
    print(f"Current average (bare) is: {get_average_bare()}.")
    print(f"Current average (Pandas) is: {get_average_pandas()}.")
    print(f"Current average (Polars) is: {get_average_polars()}.")
    # TODO: Calculate and print the percentage of records that have k2 >= 500.
