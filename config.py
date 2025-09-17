import os


DIR_DATA = "data"
os.makedirs(DIR_DATA, exist_ok=True)
PATH_DATA = os.path.join(DIR_DATA, "data.csv")
PATH_DB = os.path.join(DIR_DATA, "data.db")
