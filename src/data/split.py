import src
import numpy as np
import os
import sqlite3
import pandas as pd

data_dir = src.data_path
config_dir = src.config_path

def import_df():
    sql_path = os.path.join(data_dir, 'sql')
    ml_path = os.path.join(sql_path, "ml.sqlite")
    ml_conn = sqlite3.connect(ml_path)
    cols = ["userId", "movieId", "rating", "timestamp"]  # must include userId and timestamp
    return pd.read_sql(f"SELECT {', '.join(cols)} FROM ratings ORDER BY userId ASC, timestamp ASC", ml_conn)

def split_data():
    train_rows = np.array([], dtype=int)
    test_rows = np.array([], dtype=int)
    val_rows = np.array([], dtype=int)
    train_frac = 0.8
    test_frac = 0.1
    index = 0
    df_data = import_df()
    for userId, group in df_data.groupby("userId"):
        n_ratings = len(group)
        n_train = int(np.floor(n_ratings * train_frac))
        n_test = int(np.floor(n_ratings * test_frac))
        train_rows = np.append(train_rows, np.arange(index, index + n_train))
        test_rows = np.append(test_rows, np.arange(index + n_train, index + n_train + n_test))
        val_rows = np.append(val_rows, np.arange(index + n_train + n_test, index + n_ratings))
        index += n_ratings

    df_train = df_data.iloc[train_rows].copy()
    df_test = df_data.iloc[test_rows].copy()
    df_val = df_data.iloc[val_rows].copy()
    return df_train, df_test, df_val

def save_splits(data_dir, df_train, df_test, df_val):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    df_train.to_csv(os.path.join(data_dir, "train.csv"), index=False)
    df_test.to_csv(os.path.join(data_dir, "test.csv"), index=False)
    df_val.to_csv(os.path.join(data_dir, "val.csv"), index=False)

def main():
    split_dir = os.path.join(data_dir, "splits")
    save_splits(split_dir, *split_data())
    pass

if __name__ == "__main__":
    main()