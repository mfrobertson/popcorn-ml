import numpy as np
from narwhals import to_native
import src
import os
import src.tuning as tune
import yaml

config_dir = src.config_path
with open(os.path.join(config_dir, "tune.yaml"), 'r') as f:
    tune_config = yaml.safe_load(f)

def outdir(db_name, model_name):
    tune_dir = src.tune_path
    return os.path.join(tune_dir, db_name, model_name)

def save_params(db_name, model_name, params):
    outfile = os.path.join(outdir(db_name, model_name), "params.yml")
    with open(outfile, 'w') as f:
        yaml.dump(to_native(params), f)

def to_native(obj):
    if isinstance(obj, dict):
        return {to_native(k): to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_native(v) for v in obj]
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    return obj

def run_and_save_model(db_name, model_name, **model_kw):
    Model = tune.import_Model(model_name)
    model = Model(**model_kw)
    df_train, df_val = tune.get_data(db_name)
    user_col, item_col, rating_col = tune.get_cols(db_name)
    model.fit(df_train, user_col=user_col, item_col=item_col, rating_col=rating_col)
    outpath = outdir(db_name, model_name)
    np.save(os.path.join(outpath, "Q.npy"), model.Q_)
    np.save(os.path.join(outpath, "mu.npy"), model.mu_)
    if model_name == "FunkSVD":
        np.save(os.path.join(outpath, "bi.npy"), model.bi_)
    with open(os.path.join(outpath, "item_to_idx.yml"), 'w') as f:
        yaml.dump(to_native(model.item_to_idx_), f)

def collab_runner(db_name, model_name):
    print(f"Collaborative tuning for db: {db_name}, with model: {model_name}.")
    kwargs = {}
    for param in tune_config[db_name][model_name]:
        print(f"Tuning parameter: {param}...")

        selection = tune_config[db_name][model_name][param]["selection"]
        tol = tune.str_to_num(tune_config[db_name][model_name][param]["tol"])
        best_param = tune.main(db_name, model_name, param, tol=tol, selection=selection, **kwargs)

        print(f"  {param}: {best_param}")
        kwargs[param] = best_param

    save_params(db_name, model_name, kwargs)
    run_and_save_model(db_name, model_name, **kwargs)


if __name__ == "__main__":
    db_name = "ml-small"
    model_name = "FunkSVD"
    collab_runner(db_name, model_name)