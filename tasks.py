import csv
import math
from invoke import task
from config import PATH_DATA
from datagen import get_many
from time import time
import psutil

from db import get_average, get_average_bare, init
from utilities import timing


process = psutil.Process()


@task
def generate(_):
    N = 5000  # TODO: Make this 5m, and interpret.

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
    # TODO: How to calculate and print the rate per second?


@task
def analyse(_):
    init()
    print(f"Current average (DuckDB) is: {get_average()}.")
    print(f"Current average (bare) is: {get_average_bare()}.")
    # TODO: Calculate and print the percentage of records that have k2 >= 500.
