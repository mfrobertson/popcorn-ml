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
    models_path = os.path.join(src.models_path, db_name, model_name)
    if not os.path.isdir(models_path):
        os.makedirs(models_path)
    outfile = os.path.join(models_path, "params.yml")
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
    models_path = os.path.join(src.models_path, db_name, model_name)
    print("Saving Q")
    np.save(os.path.join(models_path, "Q.npy"), model.Q_)
    print("Saving P")
    np.save(os.path.join(models_path, "P.npy"), model.P_)
    print("Saving mu")
    np.save(os.path.join(models_path, "mu.npy"), model.mu_)
    if model_name == "FunkSVD":
        print("Saving bi")
        np.save(os.path.join(models_path, "bi.npy"), model.bi_)
        print("Saving bu")
        np.save(os.path.join(models_path, "bu.npy"), model.bu_)
    with open(os.path.join(models_path, "item_to_idx.yml"), 'w') as f:
        print("Saving item_to_index")
        yaml.dump(to_native(model.item_to_idx_), f)
    with open(os.path.join(models_path, "user_to_idx.yml"), 'w') as f:
        print("Saving user_to_index")
        yaml.dump(to_native(model.user_to_idx_), f)

def collab_runner(db_name, model_name):
    print(f"Collaborative tuning for db: {db_name}, with model: {model_name}.")
    if db_name == "ml-large" or db_name == "ml-new":
        print(f"Skipping tuning for {db_name}, using ml-small tuning results instead...")
        ml_small_outfile = os.path.join(outdir("ml-small", model_name), "params.yml")
        with open(ml_small_outfile, 'r') as f:
            kwargs = yaml.safe_load(f)
    else:
        kwargs = {}
        for param in tune_config[db_name][model_name]:
            print(f"Tuning parameter: {param}...")

            selection = tune_config[db_name][model_name][param]["selection"]
            tol = tune.str_to_num(tune_config[db_name][model_name][param]["tol"])
            best_param = tune.main(db_name, model_name, param, tol=tol, selection=selection, **kwargs)

            print(f"  {param}: {best_param}")
            kwargs[param] = best_param

    print("Saving tuned parameters")
    save_params(db_name, model_name, kwargs)
    print(f"Running model {model_name} with parameters {kwargs}.")
    run_and_save_model(db_name, model_name, **kwargs)


if __name__ == "__main__":
    db_name = "ml-new"
    model_name = "FunkSVD"
    collab_runner(db_name, model_name)