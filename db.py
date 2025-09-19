import duckdb
import pandas as pd
from config import PATH_DATA, PATH_DB
from utilities import timing
import polars as pl

VIEW = f"""
    CREATE OR REPLACE VIEW vw_data AS
    SELECT *
    FROM read_csv_auto('{PATH_DATA}');
"""


def get_con():
    return duckdb.connect(database=PATH_DB, read_only=False)


@timing
def init():
    get_con().execute(VIEW)


@timing
def get_average_duckdb():
    sql = "SELECT AVG(k2) FROM vw_data"
    return round(get_con().execute(sql).fetchone()[0], 1)


@timing
def get_average_bare():
    with open(PATH_DATA, "r") as f:
        line = f.readline()
        total = 0
        count = 0
        while True:
            line = f.readline()
            if not line:
                break
            total += float(line.split(",")[0])
            count += 1
    return round(total / count, 1)


@timing
def get_average_pandas():
    return round(pd.read_csv(PATH_DATA).k2.mean(), 1)


@timing
def get_average_polars():
    return round(pl.read_csv(PATH_DATA)["k2"].mean(), 1)
