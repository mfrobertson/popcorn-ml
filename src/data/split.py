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
    rows = [[],[],[]]

    df_data = import_df(db_name, config)

    train_frac = config["ratios"]["train"]
    val_frac = config["ratios"]["val"]
    split_col = config["split_by_group"]["col"]

    for _, group in df_data.groupby(split_col):
        n_ratings = len(group)
        n_train = int(np.floor(n_ratings * train_frac))
        n_val = int(np.floor(n_ratings * val_frac))

        index = group.index[0]
        rows[0].extend(range(index, index + n_train))
        rows[1].extend(range(index + n_train, index + n_train + n_val))
        rows[2].extend(range(index + n_train + n_val, index + n_ratings))

    return [df_data.iloc[row].copy() for row in rows]

def ensure_present_all_sets(dfs, config):
    pas_cols = config["present_all_sets"]["cols"]

    train_df = dfs[0]
    val_df = dfs[1]
    test_df = dfs[2]

    val_to_train = []
    test_to_train = []
    test_to_val = []
    for col in pas_cols:
        for groupId, group in val_df.groupby(col):
            if groupId not in train_df[col].values:
                index = group.index[0]
                val_to_train.append(index)
    train_df = pd.concat([train_df, val_df.loc[val_to_train]])
    print(f"Moved {len(val_to_train)} ratings to train from val.")
    val_df.drop(val_to_train, inplace=True)

    for col in pas_cols:
        for groupId, group in test_df.groupby(col):
            n_ratings = len(group)
            train_append = False
            if groupId not in train_df[col].values:
                index = group.index[0]
                test_to_train.append(index)
                train_append = True
            if groupId not in val_df[col].values:
                if not train_append:
                    index = group.index[0]
                elif n_ratings > 1:
                    index = group.index[1]
                else:
                    continue
                test_to_val.append(index)
    dfs[0] = pd.concat([train_df, test_df.loc[test_to_train]])
    dfs[1] = pd.concat([val_df, test_df.loc[test_to_val]])
    print(f"Moved {len(test_to_train)} ratings to train from test.")
    print(f"Moved {len(test_to_val)} ratings to val from test.")
    test_to_train.extend(test_to_val)
    dfs[2].drop(test_to_train, inplace=True)
    return dfs

def split_data(db_name):
    config = split_config[db_name]
    if config["split_by_group"]["active"]:
        dfs = split_by_col(db_name, config)
    else:
        dfs = random_split()

    if config["present_all_sets"]["active"]:
        dfs = ensure_present_all_sets(dfs, config)
    return dfs

def save_splits(data_dir, df_train, df_val, df_test):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    df_train.to_csv(os.path.join(data_dir, "train.csv"), index=False)
    df_val.to_csv(os.path.join(data_dir, "val.csv"), index=False)
    df_test.to_csv(os.path.join(data_dir, "test.csv"), index=False)

def check_config(db_name):
    assert db_name in split_config
    config = split_config[db_name]
    assert sum(config["ratios"].values()) == 1
    keys = ["split_by_group", "present_all_sets", "sql_query"]
    for key in keys:
        assert key in config

def main(db_name):
    check_config(db_name)
    split_dir = os.path.join(data_dir, "splits")
    save_splits(split_dir, *split_data(db_name))
    pass

if __name__ == "__main__":
    main(sys.argv[1])