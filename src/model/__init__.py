import src
import os
import yaml
import numpy as np
from .funk_svd import FunkSVD
from .svds import SVDs

__all__ = ["FunkSVD", "SVDs"]


def _get_Model(model_type):
    if model_type == "FunkSVD":
        return FunkSVD
    if model_type == "SVDs":
        return SVDs
    else:
        raise Exception(f"Unknown model type: {model_type}")

def get_model(model_type, database, clip=False):
    Model = _get_Model(model_type)
    model_path = os.path.join(src.models_path, database, model_type)
    with open(os.path.join(model_path, "params.yml"), 'r') as f:
        params = yaml.safe_load(f)
    with open(os.path.join(model_path, "item_to_idx.yml"), 'r') as f:
        item_to_idx = yaml.safe_load(f)
    with open(os.path.join(model_path, "user_to_idx.yml"), 'r') as f:
        user_to_idx = yaml.safe_load(f)
    if clip:
        model = Model(clip_min=0.5, clip_max=5, **params)
    else:
        model = Model(**params)
    model.item_to_idx_ = item_to_idx
    model.user_to_idx_ = user_to_idx
    model.Q_ = np.load(os.path.join(model_path, "Q.npy"))
    model.P_ = np.load(os.path.join(model_path, "P.npy"))
    model.mu_ = np.load(os.path.join(model_path, "mu.npy"))
    if model_type == "FunkSVD":
        model.bi_ = np.load(os.path.join(model_path, "bi.npy"))
        model.bu_ = np.load(os.path.join(model_path, "bu.npy"))
    return model