import datetime
import os
import numpy as np
import pandas as pd
import src
import yaml
import sys
import matplotlib.pyplot as plt


data_path = src.data_path
tune_path = src.tune_path
splits_path = os.path.join(data_path, "splits")

config_dir = src.config_path
with open(os.path.join(config_dir, "tune.yaml"), 'r') as f:
    tune_config = yaml.safe_load(f)

def RMSE(model, df_val, exists=True):
    RMSE_sqr_unnorm = 0
    for row in df_val.itertuples():
        user = getattr(row, model.user_col)
        movie = getattr(row, model.item_col)
        rating = getattr(row, model.rating_col)
        pred = model.predict(user, movie, exists)
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

def str_to_num(s):
    try:
        f = float(s)
        # Check if the float value is an integer (e.g., 12.0)
        if f.is_integer():
            return int(f)
        else:
            return f
    except ValueError:
        # If float conversion fails, it's not a valid number string
        return s

def get_model_keywords(db_name, model_name, **kwargs):
    kw_default = {param: tune_config[db_name][model_name][param]["default"] for param in tune_config[db_name][model_name]}
    for key, value in kwargs.items():
        kw_default[key] = str_to_num(value)
    return kw_default

def log(message, dir_):
    if not os.path.isdir(dir_):
        os.makedirs(dir_)
    file_path = os.path.join(dir_, "log.out")
    f = open(file_path, "a")
    f.write("[" + str(datetime.datetime.now()) + "] " + message + "\n")
    f.close()

def find_best_param(params, RMSEs, tol, selection):
    if selection == "min_param":
        func = np.min
    elif selection == "max_param":
        func = np.max
    elif selection == "min":
        func = None
    else:
        raise ValueError(f"{selection} is not a valid selection.")
    idx_sorted = np.argsort(RMSEs)
    params_sorted = params[idx_sorted]
    RMSEs_sorted = RMSEs[idx_sorted]
    if func:
        best_params = params_sorted[np.where(np.abs(RMSEs_sorted - RMSEs_sorted[0])/RMSEs_sorted[0] < tol)]
        return func(best_params)
    return params_sorted[0]

def main(db_name, model_name, parameter, tol=2e-3, selection="min", **kwargs):
    run_path = os.path.join(tune_path, db_name, model_name, parameter)
    log("---------------------------------", run_path)
    log(f"db: {db_name}, model: {model_name}, parameter: {parameter}, and kwargs: {kwargs}", run_path)

    df_train, df_val = get_data(db_name)
    params = get_params(db_name, model_name, parameter)
    Model = import_Model(model_name)
    model_kw = get_model_keywords(db_name, model_name, **kwargs)

    user_col = tune_config[db_name]["user_col"]
    item_col = tune_config[db_name]["item_col"]
    rating_col = tune_config[db_name]["rating_col"]

    log(f"Parameters: {model_kw}", run_path)
    log(f"Grid for parameter: {parameter} = {params}", run_path)
    RMSEs = np.zeros(np.size(params))
    for iii, param in enumerate(params):
        log(f"  Parameter: {parameter} = {param}", run_path)
        model_kw[parameter] = param
        model = Model(**model_kw)
        model.fit(df_train, user_col=user_col, item_col=item_col, rating_col=rating_col)
        RMSEs[iii] = RMSE(model, df_val)
        log(f"  RMSE: {RMSEs[iii]}", run_path)

    np.save(os.path.join(run_path, "rmses.npy"), RMSEs)
    np.save(os.path.join(run_path, "params.npy"), params)

    best_param = find_best_param(params, RMSEs, tol, selection)
    log(f"Best value for parameter: {parameter} = {best_param}", run_path)

    log("---------------------------------", run_path)

    plt.figure()
    plt.plot(params, RMSEs)
    plt.axvline(best_param, color="r")
    plt.title(f"{parameter} = {best_param}")
    plt.xlabel(f"{parameter}")
    plt.ylabel("RMSE")
    plt.savefig(os.path.join(run_path, f"param_v_rmse.pdf"))

    return best_param


if __name__ == '__main__':
    n_req_args = 3
    args = sys.argv[1:n_req_args+1]
    kwargs = dict(arg.split('=') for arg in sys.argv[n_req_args+1:])
    main(*args, **kwargs)