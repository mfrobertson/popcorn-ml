import streamlit as st
from src.tmdb import TMDB
import pandas as pd
import src
import os
import numpy as np
from src.st_helpers import display_image_with_hover, display_image_with_hover_rated
from src.model import svds, funk_svd
import yaml


src_dir = src.src_path

Ncol = 5
Nrow = 20

@st.cache_data()
def get_data():
    df_mappings = pd.read_json(os.path.join(src_dir, "data", "movie_mappings.json"))
    df_pop = pd.read_json(os.path.join(src_dir, "data", "popular_movies.json"))
    df_pop = df_pop.merge(df_mappings, on="movieId", how="left")
    tmdbId_to_movieId = dict(zip(df_mappings["tmdbId"], df_mappings["movieId"]))
    movieId_to_tmdbId = dict(zip(df_mappings["movieId"], df_mappings["tmdbId"]))
    # movieId_to_imdbId = dict(zip(df_mappings["movieId"], df_mappings["imdbId"]))
    return df_mappings, df_pop, tmdbId_to_movieId, movieId_to_tmdbId

if "tmdb" not in st.session_state:
    st.session_state.tmdb = TMDB(media_type="movie")
    locale = st.context.locale
    if locale:
        st.session_state.tmdb.cc = locale.split("-")[1]  # Country code from users browser (regardless of location)
    if not st.session_state.tmdb.API_KEY:
        st.session_state.tmdb.API_KEY = st.secrets.get("TMDB_KEY")

def initialise_rows():
    st.session_state[f"button_0_disabled"] = True
    for i in range(0, Nrow):
        st.session_state[f"button_{i}_disabled"] = False
    st.session_state.row = 0
    for i in range(Ncol * Nrow):
        st.session_state[f"slider_{i}"] = 0

def rating_slider(col_idx):
    rating_values = [i for i in range(0,11)]
    st.select_slider("Rating", rating_values, key=f"slider_{col_idx}", label_visibility="collapsed")

@st.fragment()
def display_movie_with_slider(col_idx):
    poster_path, name, date, _ = rand_movie(col_idx)
    rated = True if st.session_state[f"slider_{col_idx}"] != 0 else False
    display_image_with_hover_rated(_tmdb.get_poster_fullpath(poster_path, poster_size),f"{name} ({date.split("-")[0]})", rated)
    rating_slider(col_idx)

@st.fragment()
def ratings():
    cols = st.columns(Ncol)
    for iii, col in enumerate(cols):
        with col:
            col_idx = iii + (st.session_state.row * Ncol)
            display_movie_with_slider(col_idx)

@st.fragment()
def more_button(row):
    if not st.session_state[f"button_{row}_disabled"]:
        st.button("More", key=f"more_{row}")
    else:
        if st.session_state.row < Nrow:
            st.session_state.row += 1
            show_ratings()
        else:
            st.toast("Max number of movies reached.")

@st.fragment()
def show_ratings():
    row = st.session_state.row
    ratings()
    more_button(row)
    st.session_state[f"button_{row}_disabled"] = True

def enough_data(user_data):
    N_ratings = len(user_data)
    min_ratings = 5
    if N_ratings < min_ratings:
        st.toast(f"Rate at least {5 - N_ratings} more movies.")
        return False
    return True

@st.fragment()
def submit_button():
    Nrows = st.session_state.row + 1
    if st.button("Submit"):
        user_data = {int(popular_tmdbIds[rand_indices[col_idx]]): st.session_state[f"slider_{col_idx}"] / 2 for col_idx in np.arange(Ncol * Nrows) if st.session_state[f"slider_{col_idx}"] != 0}
        if enough_data(user_data):
            movieIds, pred_ratings = get_predictions(user_data)
            show_results(movieIds, pred_ratings, user_data)

@st.fragment()
def show_results(movieIds, pred_ratings, user_data):
    with st.container(border=True):
        st.markdown("#### Personalised Recommendations")
        sorted_idx = np.argsort(pred_ratings)[::-1]
        cols = st.columns(Ncol)
        col_idx = 0
        for col in cols:
            poster_path = None
            with col:
                # If movie not in tmdb database, or already rated, move on to next
                while poster_path is None:
                    try:
                        poster_path, name, date, tmdbId = get_movie(int(sorted_idx[col_idx]), movieIds)
                        if tmdbId in list(user_data.keys()):
                            poster_path = None
                            raise KeyError("Suggested movie already rated by user")
                    except KeyError:
                        col_idx += 1
                display_image_with_hover(_tmdb.get_poster_fullpath(poster_path, poster_size), f"{name} ({date.split("-")[0]})")
                st.markdown("######")
                col_idx += 1

def get_Model(model_type):
    if model_type == "FunkSVD":
        return funk_svd.FunkSVD
    if model_type == "SVDs":
        return svds.SVDs
    else:
        raise Exception(f"Unknown model type: {model_type}")

def setup_model(model_type, database, clip=False):
    Model = get_Model(model_type)
    models_path = os.path.join(src.models_path, database, model_type)
    with open(os.path.join(models_path, "params.yml"), 'r') as f:
        params = yaml.safe_load(f)
    with open(os.path.join(models_path, "item_to_idx.yml"), 'r') as f:
        item_to_idx = yaml.safe_load(f)
    if clip:
        model = Model(clip_min=0.5, clip_max=5, **params)
    else:
        model = Model(**params)
    model.item_to_idx_ = item_to_idx
    model.Q_ = np.load(os.path.join(models_path, "Q.npy"))
    model.mu_ = np.load(os.path.join(models_path, "mu.npy"))
    if model_type == "FunkSVD":
        model.bi_ = np.load(os.path.join(models_path, "bi.npy"))
    return model

def get_predictions(user_data, model_type="FunkSVD", database="ml-new"):
    model = setup_model(model_type, database)
    items = [id_mapping(tmdbId=tmdbId) for tmdbId in list(user_data.keys())]
    model.add_new_user(items, list(user_data.values()))
    movieIds = list(model.item_to_idx_.keys())
    return movieIds, model.new_user_predictions(movieIds)

def get_movie(idx, movie_list=None):
    if movie_list is None:
        tmdbId = popular_tmdbIds[idx]
    else:
        movieId = movie_list[idx]
        tmdbId = id_mapping(movieId=movieId)
    if tmdbId == "nan":
        raise KeyError("No tmdbId exists for this movie.")
    movie_json = _tmdb.get_details_by_id(tmdbId, "movie")
    return movie_json["poster_path"], movie_json["title"], movie_json["release_date"], tmdbId

def rand_movie(col_idx):
    return get_movie(rand_indices[col_idx])

def id_mapping(tmdbId=None, movieId=None):
    if tmdbId is None and movieId is None:
        raise ValueError("Must specify either tmdbId or movieId")
    if tmdbId is not None:
        return int(tmdbId_to_movieId[tmdbId])
    return int(movieId_to_tmdbId[movieId])

df_mappings, df_pop, tmdbId_to_movieId, movieId_to_tmdbId = get_data()
popular_tmdbIds = df_pop["tmdbId"]
N_pop = np.size(popular_tmdbIds)
rand_indices = np.random.choice(np.arange(N_pop), size=N_pop, replace=False)

_tmdb = st.session_state.tmdb
poster_size = "w154"

initialise_rows()
with st.container(border=True):
    st.markdown("#### Rate Movies", help="Personalised movie recommendations. Rate at least 5 movies (the more you rate the better the recommendations) and submit to display suggested films.")
    show_ratings()
submit_button()
