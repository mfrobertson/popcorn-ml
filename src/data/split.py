import src
import numpy as np
import os
import sqlite3
import pandas as pd
import yaml
import sys

data_dir = src.data_path
config_dir = src.config_path
with open(os.path.join(config_dir, "split.yaml"), 'r') as f:
    split_config = yaml.safe_load(f)

def import_df(db_name, config):
    sql_path = os.path.join(data_dir, 'sql')
    ml_path = os.path.join(sql_path, f"{db_name}.sqlite")
    ml_conn = sqlite3.connect(ml_path)
    sql_query = config["sql_query"]
    return pd.read_sql(sql_query, ml_conn)

def random_split():
    print("Random split not yet impemented.")
    return None, None, None

def split_by_col(db_name, config):
    train_rows = np.array([], dtype=int)
    test_rows = np.array([], dtype=int)
    val_rows = np.array([], dtype=int)

    df_data = import_df(db_name, config)

    train_frac = config["ratios"]["train"]
    test_frac = config["ratios"]["test"]
    split_col = config["split_by"]

    index = 0
    for _, group in df_data.groupby(split_col):
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

def ensure_present_all_sets(dfs, config):
    print("Ensure present all sets not yet implemented.")
    return dfs

def split_data(db_name):
    config = split_config[db_name]
    if config["split_by"]:
        dfs = split_by_col(db_name, config)
    else:
        dfs = random_split()

    if config["present_all_sets"]:
        dfs = ensure_present_all_sets(dfs, config)
    return dfs

def save_splits(data_dir, df_train, df_test, df_val):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    df_train.to_csv(os.path.join(data_dir, "train.csv"), index=False)
    df_test.to_csv(os.path.join(data_dir, "test.csv"), index=False)
    df_val.to_csv(os.path.join(data_dir, "val.csv"), index=False)

def main(db_name):
    split_dir = os.path.join(data_dir, "splits")
    save_splits(split_dir, *split_data(db_name))
    pass

if __name__ == "__main__":
    main(sys.argv[1])