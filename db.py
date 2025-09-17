import duckdb

from config import PATH_DATA, PATH_DB
from utilities import timing

VIEW = f"""
    CREATE OR REPLACE VIEW vw_data AS
    SELECT *
    FROM read_csv_auto('{PATH_DATA}');
"""


def _get_con():
    return duckdb.connect(database=PATH_DB, read_only=False)


@timing
def init():
    _get_con().execute(VIEW)


@timing
def get_average():
    sql = "SELECT AVG(k2) FROM vw_data"
    return round(_get_con().execute(sql).fetchone()[0], 1)


@timing
def get_average_bare():
    with open(PATH_DATA, "r") as f:
        f.readline()
        line = f.readline()
        total = 0
        count = 0
        while line:
            total += float(line.split(",")[0])
            count += 1
            line = f.readline()
    return round(total / count, 1)
