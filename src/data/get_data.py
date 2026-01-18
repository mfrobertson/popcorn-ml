import sys
from io import BytesIO
import zipfile
import requests
import gzip
import shutil
import os
import sqlite3
import pandas as pd
from src import data_path

DATA_PATH = data_path
IMDB_EXT = "imdb"
ML_EXT = "ml"
SQL_EXT = "sql"

def ml_get(type):
    if type == "small":
        dir_name_orig = "ml-latest-small"
    elif type == "latest":
        dir_name_orig = "ml-latest"
    elif type == "new":
        dir_name_orig = "ml-32m"
    else:
        raise ValueError(f"Unknown type for movieLens dataset: {type}.")

    url = f"https://files.grouplens.org/datasets/movielens/{dir_name_orig}.zip"

    ml_dir = DATA_PATH

    print("Downloading ml data...")
    data_get = requests.get(url)
    data_get.raise_for_status()

    data_zip = zipfile.ZipFile(BytesIO(data_get.content))
    data_zip.extractall(ml_dir)

    dir_name = f"ml-{type}"
    os.rename(os.path.join(ml_dir, dir_name_orig), os.path.join(ml_dir, dir_name))
    print(f"Completed downloading {dir_name} data.")

def imdb_get():
    url = "https://datasets.imdbws.com/"
    data_names = ["name.basics", "title.akas", "title.basics", "title.crew",
                  "title.episode", "title.principals", "title.ratings"]

    imdb_dir = os.path.join(DATA_PATH, IMDB_EXT)
    os.makedirs(imdb_dir, exist_ok=True)

    for name in data_names:
        print("Downloading " + name + " imdb data...")
        data_get = requests.get(url + name + ".tsv.gz", stream=True)
        data_get.raise_for_status()

        print("Extracting " + name + " gzip...")
        with gzip.GzipFile(fileobj=data_get.raw) as gz:
            file_path = os.path.join(imdb_dir, name + ".tsv")
            with open(file_path, "wb") as f:
                shutil.copyfileobj(gz, f)
        print("Completed " + name + ".")
    print("Completed imdb download.")

def load_to_sql(name):

    def data_to_sql(dir_path, db):
        def get_sep(file_type):
            if file_type == "csv":
                return ","
            if file_type == "tsv":
                return "\t"
            raise ValueError("Unsupported file type: " + file_type)
        def filename_type(file):
            file_split = file.split(".")
            file_type = file_split[-1]
            filename = ".".join(file_split[:-1])
            return filename, file_type

        for file in os.listdir(dir_path):
            tablename, file_type = filename_type(file)
            try: sep = get_sep(file_type)
            except: continue
            print("  Processing " + tablename)
            df = pd.read_csv(os.path.join(dir_path, file), sep=sep, header=0)
            df.to_sql(tablename, db, if_exists="replace", index=False, chunksize=10000, method="multi")

    sql_dir = os.path.join(DATA_PATH, SQL_EXT)
    data_dir = os.path.join(DATA_PATH, name)

    os.makedirs(sql_dir, exist_ok=True)

    db_name = name + ".sqlite"
    sql_conn = sqlite3.connect(os.path.join(sql_dir, db_name))

    print(f"Loading {name} data to sql...")
    data_to_sql(data_dir, sql_conn)
    print(f"Completed loading {name} data to sql.")

    sql_conn.close()

def remove_dirs(*dirs):
    for dir in dirs:
        dir_path = os.path.join(DATA_PATH, dir)
        print("Removing " + dir_path)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)

def import_data(*names):
    for name in names:
        if name == "imdb":
            imdb_get()
        elif name[:2] == "ml":
            type = name.split("-")[1]
            ml_get(type)
        else:
            raise ValueError(f"Unknown data source: {name}")
        load_to_sql(name)


def main(*names):
    import_data(*names)
    remove_dirs(*names)


if __name__ == "__main__":
    main(*sys.argv[1:])