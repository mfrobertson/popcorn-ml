from io import BytesIO
import zipfile
import requests
import gzip
import shutil
import os

DATA_PATH = "../data/"

def ml_get(dataset="small"):
    urls = {
        "large": "https://files.grouplens.org/datasets/movielens/ml-latest.zip",
        "small": "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"
    }

    path_ext = "ml"
    ml_dir = os.path.join(DATA_PATH, path_ext)

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
    path_ext = "imdb"
    imdb_dir = os.path.join(DATA_PATH, path_ext)
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


if __name__ == "__main__":
    ml_get("small")
    imdb_get()