from narwhals import to_native

import src
import os
import src.tuning as tune
import yaml

config_dir = src.config_path
with open(os.path.join(config_dir, "tune.yaml"), 'r') as f:
    tune_config = yaml.safe_load(f)

def save_params(db_name, model_name, params):
    tune_dir = src.tune_path
    outdir = os.path.join(tune_dir, db_name, model_name)
    with open(os.path.join(outdir, "params.yml"), 'w') as outfile:
        yaml.dump(to_native(params), outfile)

def to_native(obj):
    if isinstance(obj, dict):
        return {k: to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [to_native(v) for v in obj]
    if hasattr(obj, "item"):  # numpy scalar
        return obj.item()
    return obj

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


if __name__ == "__main__":
    db_name = "ml-small"
    model_name = "FunkSVD"
    collab_runner(db_name, model_name)