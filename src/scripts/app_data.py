import sqlite3
import pandas as pd
import os
import src
import numpy as np

data_dir = src.data_path
src_dir = src.src_path
sql_path = os.path.join(data_dir, "sql")
outdir = os.path.join(src_dir, "data")


def main(db_name):
    ml_path = os.path.join(sql_path, f"{db_name}.sqlite")
    ml_conn = sqlite3.connect(ml_path)

    tables = ml_conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    print(f"Table names: {tables}")

    for table in tables:
        count = ml_conn.execute(f'SELECT COUNT(*) FROM "{table[0]}"').fetchall()[0][0]
        print(f"{count} rows in {table[0]}")

    # colums of each table
    for table in tables:
        df = pd.read_sql(f'SELECT * FROM "{table[0]}" LIMIT 1', ml_conn)
        print(f"{table[0]} has columns: {df.columns.tolist()}")

    df_mappings = pd.read_sql(f"SELECT * FROM links", ml_conn)
    df_mappings.to_json(os.path.join(outdir, "movie_mappings.json"))

    df_train = pd.read_csv(os.path.join(data_dir, "splits", db_name, "train.csv"))
    popular_200_movies = df_train.groupby("movieId").size().sort_values(ascending=False).head(200)
    print(f"10 most rated movies in training set:")
    print(popular_200_movies[:10])
    pd.DataFrame({"movieId": popular_200_movies.index}).to_json(os.path.join(outdir, "popular_movies.json"))


if __name__ == '__main__':
    main("ml-new")