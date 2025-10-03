from io import BytesIO
import zipfile
import requests
import gzip
import shutil
import os
import sqlite3
import pandas as pd

DATA_PATH = "../data/"
IMDB_EXT = "imdb"
ML_EXT = "ml"

def ml_get(dataset="small"):
    urls = {
        "large": "https://files.grouplens.org/datasets/movielens/ml-latest.zip",
        "small": "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    }

    ml_dir = os.path.join(DATA_PATH, ML_EXT)

    print("Downloading ml data...")
    data_get = requests.get(urls[dataset])
    data_get.raise_for_status()

    data_zip = zipfile.ZipFile(BytesIO(data_get.content))
    data_zip.extractall(ml_dir)
    print("Completed downloading ml data.")

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

def load_to_sql(ml_type="small"):

    ml_db_path = {"small": "ml-latest-small", "large": "ml-latest"}

    def data_to_sql(dir_path, db, sep=","):
        def get_sep(file_type):
            if file_type == "csv":
                return ","
            if file_type == "tsv":
                return "\t"
            raise ValueError("Unsupported file type: " + file_type)

        for file in os.listdir(dir_path):
            file_name = file[:-4]
            file_type = file[-3:]
            try: sep = get_sep(file_type)
            except: continue
            print("Processing " + file_name)
            df = pd.read_csv(os.path.join(dir_path, file), sep=sep, header=0)
            df.to_sql(file_name, db, if_exists="replace", index=False, chunksize=10000, method="multi")

    path_ext = "sql"
    sql_dir = os.path.join(DATA_PATH, path_ext)
    ml_dir = os.path.join(DATA_PATH, ML_EXT)
    imdb_dir = os.path.join(DATA_PATH, IMDB_EXT)

    os.makedirs(sql_dir, exist_ok=True)

    ml_conn = sqlite3.connect(os.path.join(sql_dir, "ml.sqlite"))
    imdb_conn = sqlite3.connect(os.path.join(sql_dir, "imdb.sqlite"))

    print("Loading ml data to sql...")
    data_to_sql(os.path.join(ml_dir, ml_db_path[ml_type]), ml_conn)
    print("Completed loading ml data to sql.")

    print("Loading imdb data to sql...")
    data_to_sql(imdb_dir, imdb_conn, sep="\t")
    print("Completed loading imdb data to sql.")

    ml_conn.close()
    imdb_conn.close()


if __name__ == "__main__":
    ml_get("small")
    imdb_get()
    load_to_sql()