import sqlite3
import os
import numpy as np
import pandas as pd
import src
import yaml


data_path = src.data_path
splits_path = os.path.join(data_path, "splits")

config_dir = src.config_path
with open(os.path.join(config_dir, "tune.yaml"), 'r') as f:
    tune_config = yaml.safe_load(f)

def RMSE(model, df_val, exits=True):
    RMSE_sqr_unnorm = 0
    for row in df_val.itertuples():
        user = getattr(row, model.user_col)
        movie = getattr(row, model.item_col)
        rating = getattr(row, model.rating_col)
        pred = model.predict(user, movie, exits)
        RMSE_sqr_unnorm += (rating - pred) ** 2
    return np.sqrt(RMSE_sqr_unnorm/len(df_val))

def get_data(db_name):
    train_path = os.path.join(splits_path, db_name, "train.csv")
    val_path = os.path.join(splits_path, db_name, "val.csv")
    df_train = pd.read_csv(train_path)
    df_val = pd.read_csv(val_path)
    return df_train, df_val

def get_params(db_name, model_name, parameter):
    config = tune_config[db_name][model_name]
    params_min = config[parameter]["min"]
    params_max = config[parameter]["max"]
    params_step_size = config[parameter]["step_size"]
    return np.arange(params_min, params_max, params_step_size)

def import_Model(model_name):
    if model_name == "FunkSVD":
        from src.model.funk_svd import FunkSVD as Model
    elif model_name == "SVDs":
        from src.model.svds import SVDs as Model
    else:
        raise ValueError(f"{model_name} is not a valid model name.")
    return Model

def get_model_keywords(db_name, model_name, **kwargs):
    kw_default = {param: tune_config[db_name][model_name][param]["default"] for param in tune_config[db_name][model_name]}
    for key, value in kwargs.items():
        kw_default[key] = value
    return kw_default


def main(db_name, model_name, parameter, **kwargs):
    df_train, df_val = get_data(db_name)
    params = get_params(db_name, model_name, parameter)
    Model = import_Model(model_name)
    model_kw = get_model_keywords(db_name, model_name, **kwargs)

    user_col = tune_config[db_name]["user_col"]
    item_col = tune_config[db_name]["item_col"]
    rating_col = tune_config[db_name]["rating_col"]

    for param in params:
        model_kw[parameter] = param
        model = Model(**model_kw)
        model.fit(df_train, user_col=user_col, item_col=item_col, rating_col=rating_col)
        print(param, RMSE(model, df_val))

if __name__ == '__main__':
    main("ml-small", "FunkSVD", "n_factors", lr=0.005)