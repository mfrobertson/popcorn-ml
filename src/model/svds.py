from scipy.sparse.linalg import svds
import pandas as pd
import numpy as np


class SVDs:

    def __init__(self, k: int, clip_min: float | None = None, clip_max: float | None = None):
        self.k = k
        self.clip_min = clip_min
        self.clip_max = clip_max

        self.P_ = None
        self.Q_ = None

        self.user_to_idx_ = None
        self.item_to_idx_ = None
        self.idx_to_user_ = None
        self.idx_to_item_ = None

        self.avg_rating_by_item_ = None
        self.mu_ = None



    def fit(self, df: pd.DataFrame,
        user_col: str = "user_id",
        item_col: str = "item_id",
        rating_col: str = "rating",
    ):

        users = df[user_col].unique()
        items = df[item_col].unique()

        self.user_to_idx_ = {user: iii for iii, user in enumerate(users)}
        self.item_to_idx_ = {item: iii for iii, item in enumerate(items)}
        self.idx_to_user_ = {ix: u for u, ix in self.user_to_idx_.items()}
        self.idx_to_item_ = {jx: i for i, jx in self.item_to_idx_.items()}

        self.avg_rating_by_item_ = df.groupby(item_col)[rating_col].mean().reindex(items).to_numpy()
        self.mu_ = df[rating_col].mean()

        n_users = np.size(users)

        # Creating n_users x n_movies matrix initialized with average ratings
        R = np.repeat(self.avg_rating_by_item_[None, :], n_users, axis=0)

        # Where a rating exists, input to matrix
        for row in df.itertuples():
            user_idx = self.user_to_idx_[getattr(row, user_col)]
            item_idx = self.item_to_idx_[getattr(row, item_col)]
            R[user_idx, item_idx] = getattr(row, rating_col)

        # Better performance with mean subtracted
        R -= self.mu_

        # Fitting for user and item latent matrices
        U, s, V_t = svds(R, self.k)
        idx = np.argsort(s)[::-1]
        Sig = np.diag(s[idx])
        self.P_ = U[:, idx] @ np.sqrt(Sig)
        self.Q_ = V_t[idx, :].transpose() @ np.sqrt(Sig)

    def predict(self, user_id, item_id, exists=True) -> float:
        if self.user_to_idx_ is None:
            raise RuntimeError("Model not fitted. Call fit() first.")
        if user_id not in self.user_to_idx_:
            if exists:
                raise KeyError(f"{user_id} not in user_to_idx_.")
            if item_id not in self.item_to_idx_:
                return self.mu_
            return self.avg_rating_by_item_[item_id]
        if item_id not in self.item_to_idx_:
            if exists:
                raise KeyError(f"{item_id} not in item_to_idx_.")
            return self.mu_
        user_idx = self.user_to_idx_[user_id]
        item_idx = self.item_to_idx_[item_id]

        pred = np.dot(self.P_[user_idx], self.Q_[item_idx])
        if self.clip_min is not None or self.clip_max is not None:
            pred = float(np.clip(pred,
                                 self.clip_min if self.clip_min is not None else -np.inf,
                                 self.clip_max if self.clip_max is not None else  np.inf))
        return pred + self.mu_
