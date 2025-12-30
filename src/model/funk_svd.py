import numpy as np
import pandas as pd
from numba import njit

class FunkSVD:
    """
    Biased matrix factorisation model trained with stochastic gradient descent
    (commonly referred to as Funk-SVD).

    This class learns low-dimensional latent representations for users and items
    directly from observed ratings, without ever constructing or decomposing a
    full user–item matrix.

    The model assumes the following form for each observed rating:

        r̂_ui = μ + b_u + b_i + p_uᵀ q_i

    where:
        μ     : global mean rating
        b_u   : user-specific bias
        b_i   : item-specific bias
        p_u   : k-dimensional latent vector for user u
        q_i   : k-dimensional latent vector for item i

    The parameters (μ, b_u, b_i, p_u, q_i) are learned by minimising the
    regularised squared error over observed ratings only:

        L = Σ_(u,i) (r_ui − r̂_ui)²
            + λ ( ||p_u||² + ||q_i||² + b_u² + b_i² )

    Optimisation is performed using stochastic gradient descent (SGD).

    Notes
    -----
    • This implementation is intended as a conceptual and practical replacement
      for truncated SVD (`scipy.sparse.linalg.svds`) in recommender systems.
    • Unlike SVD, this model:
        - does NOT require NaN imputation
        - trains only on observed (user, item, rating) triples
        - supports bias terms, which significantly improve accuracy
    • Cold-start users/items are not handled in this minimal implementation.

    Parameters
    ----------
    n_factors : int, default=50
        Dimensionality of the latent factor space (k).

    n_epochs : int, default=30
        Number of full passes over the training data.

    lr : float, default=0.01
        Learning rate for SGD updates.

    reg : float, default=0.02
        L2 regularisation strength applied to latent vectors and bias terms.

    random_state : int, default=42
        Seed for random number generation.

    init_std : float, default=0.01
        Standard deviation of the normal distribution used to initialise
        latent factors.

    clip_min : float or None, default=None
        Minimum allowed predicted rating. If None, no lower clipping is applied.

    clip_max : float or None, default=None
        Maximum allowed predicted rating. If None, no upper clipping is applied.

    Attributes
    ----------
    mu_ : float
        Global mean rating computed from the training data.

    bu_ : ndarray of shape (n_users,)
        Learned user bias terms.

    bi_ : ndarray of shape (n_items,)
        Learned item bias terms.

    P_ : ndarray of shape (n_users, n_factors)
        User latent factor matrix.

    Q_ : ndarray of shape (n_items, n_factors)
        Item latent factor matrix.

    user_to_idx_ : dict
        Mapping from original user IDs to internal integer indices.

    item_to_idx_ : dict
        Mapping from original item IDs to internal integer indices.

    idx_to_user_ : dict
        Reverse mapping from internal user indices to original user IDs.

    idx_to_item_ : dict
        Reverse mapping from internal item indices to original item IDs.

    Methods
    -------
    fit(df, user_col='user_id', item_col='item_id', rating_col='rating')
        Train the model using observed ratings only.

    predict(user_id, item_id)
        Predict the rating for a known user–item pair.

    factors()
        Return the learned latent factors and bias terms.

    Examples
    --------
    >>> model = FunkSVD(n_factors=30, n_epochs=20)
    >>> model.fit(train_df,
    ...           user_col="user_id",
    ...           item_col="movie_id",
    ...           rating_col="rating")
    >>> model.predict(user_id=1, item_id=50)

    See Also
    --------
    scipy.sparse.linalg.svds :
        Truncated SVD for dense or imputed matrices.

    References
    ----------
    Koren, Y., Bell, R., & Volinsky, C. (2009).
    "Matrix Factorization Techniques for Recommender Systems".
    IEEE Computer, 42(8), 30–37.

    Funk, S. (2006).
    "Netflix Update: Try This at Home".

    Written by ChatGPT 5.2
    """


    def __init__(
        self,
        n_factors: int = 50,
        n_epochs: int = 30,
        lr: float = 0.01,
        reg: float = 0.02,
        random_state: int = 42,
        init_std: float = 0.01,
        clip_min: float | None = None,
        clip_max: float | None = None,
    ):
        self.n_factors = n_factors
        self.n_epochs = n_epochs
        self.lr = lr
        self.reg = reg
        self.random_state = random_state
        self.init_std = init_std
        self.clip_min = clip_min
        self.clip_max = clip_max

        # learned parameters
        self.mu_ = None                 # global mean
        self.bu_ = None                 # (n_users,)
        self.bi_ = None                 # (n_items,)
        self.P_ = None                  # (n_users, k)
        self.Q_ = None                  # (n_items, k)

        # id->index maps
        self.user_to_idx_ = None
        self.item_to_idx_ = None
        self.idx_to_user_ = None
        self.idx_to_item_ = None

        self.user_col = None
        self.item_col = None
        self.rating_col = None

    def fit(
        self,
        df: pd.DataFrame,
        user_col: str = "user_id",
        item_col: str = "item_id",
        rating_col: str = "rating",
        shuffle: bool = True,
        verbose: bool = False,
    ):
        @njit()
        def rating_loop(order, u_idx, i_idx, r, mu_, bu_, bi_, P_, Q_, lr, reg):
            for t in order:
                u = u_idx[t]
                i = i_idx[t]
                r_ui = r[t]

                # prediction
                pred = mu_ + bu_[u] + bi_[i] + float(P_[u] @ Q_[i])
                err = r_ui - pred

                # cache old vectors for simultaneous update
                pu = P_[u].copy()
                qi = Q_[i].copy()

                # SGD updates (L2 regularisation)
                bu_[u] += lr * (err - reg * bu_[u])
                bi_[i] += lr * (err - reg * bi_[i])
                P_[u]  += lr * (err * qi - reg * pu)
                Q_[i]  += lr * (err * pu - reg * qi)
            return mu_, bu_, bi_, P_, Q_, lr, reg

        rng = np.random.default_rng(self.random_state)

        self.user_col = user_col
        self.item_col = item_col
        self.rating_col = rating_col

        users = df[user_col].unique()
        items = df[item_col].unique()
        self.user_to_idx_ = {u: ix for ix, u in enumerate(users)}
        self.item_to_idx_ = {i: jx for jx, i in enumerate(items)}
        self.idx_to_user_ = {ix: u for u, ix in self.user_to_idx_.items()}
        self.idx_to_item_ = {jx: i for i, jx in self.item_to_idx_.items()}

        n_users = len(users)
        n_items = len(items)

        # global mean from training data
        self.mu_ = float(df[rating_col].mean())

        # initialise parameters
        self.bu_ = np.zeros(n_users, dtype=np.float32)
        self.bi_ = np.zeros(n_items, dtype=np.float32)
        self.P_ = (self.init_std * rng.standard_normal((n_users, self.n_factors))).astype(np.float32)
        self.Q_ = (self.init_std * rng.standard_normal((n_items, self.n_factors))).astype(np.float32)

        # convert to indexed arrays for speed
        u_idx = df[user_col].map(self.user_to_idx_).to_numpy(dtype=np.int32)
        i_idx = df[item_col].map(self.item_to_idx_).to_numpy(dtype=np.int32)
        r = df[rating_col].to_numpy(dtype=np.float32)

        n = len(r)
        order = np.arange(n)

        for epoch in range(self.n_epochs):
            if shuffle:
                rng.shuffle(order)

            rating_loop(order, u_idx, i_idx, r, self.mu_, self.bu_, self.bi_, self.P_, self.Q_, self.lr, self.reg)

            if verbose:
                # quick training RMSE estimate (on train)
                preds = self.mu_ + self.bu_[u_idx] + self.bi_[i_idx] + np.sum(self.P_[u_idx] * self.Q_[i_idx], axis=1)
                if self.clip_min is not None or self.clip_max is not None:
                    preds = np.clip(preds,
                                    self.clip_min if self.clip_min is not None else -np.inf,
                                    self.clip_max if self.clip_max is not None else  np.inf)
                rmse = float(np.sqrt(np.mean((r - preds) ** 2)))
                print(f"epoch {epoch+1}/{self.n_epochs} train RMSE={rmse:.4f}")

        return self

    def predict(self, user_id, item_id, must_exist=True) -> float:
        """Predict a rating for a user_id, item_id seen during training. If not seen in training, return biased means"""
        if self.user_to_idx_ is None:
            raise RuntimeError("Model not fitted. Call fit() first.")
        if user_id not in self.user_to_idx_:
            if must_exist:
                raise KeyError(f"{user_id} not in user_to_idx_.")
            if item_id not in self.item_to_idx_:
                return self.mu_
            return self.mu_ + self.bi_[self.item_to_idx_[item_id]]
        if item_id not in self.item_to_idx_:
            if must_exist:
                raise KeyError(f"{item_id} not in item_to_idx_.")
            return self.mu_ + self.bu_[self.user_to_idx_[user_id]]

        u = self.user_to_idx_[user_id]
        i = self.item_to_idx_[item_id]
        pred = self.mu_ + self.bu_[u] + self.bi_[i] + float(self.P_[u] @ self.Q_[i])

        if self.clip_min is not None or self.clip_max is not None:
            pred = float(np.clip(pred,
                                 self.clip_min if self.clip_min is not None else -np.inf,
                                 self.clip_max if self.clip_max is not None else  np.inf))
        return float(pred)

    def factors(self):
        """
        Return learned factors analogous to 'getting U,S,Vt' output:
          - P_ : user factors (n_users, k)
          - Q_ : item factors (n_items, k)
          - bu_, bi_, mu_ for biases
        """
        if self.P_ is None:
            raise RuntimeError("Model not fitted.")
        return self.P_, self.Q_, self.bu_, self.bi_, self.mu_
