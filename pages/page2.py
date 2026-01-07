import streamlit as st
from src.tmdb import TMDB
import pandas as pd
import src
import os
import numpy as np
from src.st_helpers import display_image_with_hover


src_dir = src.src_path

Ncol = 5
Nrow = 20

@st.cache_data()
def get_data():
    df_mappings = pd.read_json(os.path.join(src_dir, "data", "movie_mappings.json"))
    df_pop = pd.read_json(os.path.join(src_dir, "data", "popular_movies.json"))
    df_pop = df_pop.merge(df_mappings, on="movieId", how="left")
    return df_mappings, df_pop

if "tmdb" not in st.session_state:
    st.session_state.tmdb = TMDB(media_type="movie")
    locale = st.context.locale
    if locale:
        st.session_state.tmdb.cc = locale.split("-")[1]  # Country code from users browser (regardless of location)
    if not st.session_state.tmdb.API_KEY:
        st.session_state.tmdb.API_KEY = st.secrets.get("TMDB_KEY")
if "row" not in st.session_state:
    st.session_state.row = 1

for i in range(Ncol*Nrow):
    if f"slider_{i}" not in st.session_state:
        st.session_state[f"slider_{i}"] = 0

if f"button_0_disabled" not in st.session_state:
    st.session_state[f"button_0_disabled"] = True
for i in range(1, Nrow):
    if f"button_{i}_disabled" not in st.session_state:
        st.session_state[f"button_{i}_disabled"] = False

def rating_slider(col_idx):
    rating_values = [i for i in range(0,11)]
    st.select_slider("Rating", rating_values, key=f"slider_{col_idx}", label_visibility="collapsed")

@st.fragment()
def dispay_movie_with_slider(col_idx):
    poster_path, name, date = rand_movie(col_idx)
    rated = True if st.session_state[f"slider_{col_idx}"] != 0 else False
    display_image_with_hover(f"https://image.tmdb.org/t/p/{poster_size}/{poster_path}.png?api_key={_tmdb.API_KEY}",f"{name} ({date.split("-")[0]})", rated)
    rating_slider(col_idx)

@st.fragment()
def ratings():
    cols = st.columns(Ncol)
    for iii, col in enumerate(cols):
        with col:
            col_idx = iii + (st.session_state.row * Ncol)
            dispay_movie_with_slider(col_idx)

@st.fragment()
def more_button(row):
    if not st.session_state[f"button_{row}_disabled"]:
        st.button("More", key=f"more_{row}")
    else:
        st.session_state[f"button_{row}_disabled"] = True
        st.session_state.row += 1
        if st.session_state.row < Nrow:
            show_ratings()
        else:
            st.toast("Max number of movies reached.")

@st.fragment()
def show_ratings():
    row = st.session_state.row
    ratings()
    more_button(row)
    st.session_state[f"button_{row}_disabled"] = True

def rand_movie(col_idx):
    movieId = popular_movies[rand_indices[col_idx]]
    movie_json = _tmdb.get_details_by_id(movieId, "movie")
    return movie_json["poster_path"], movie_json["title"], movie_json["release_date"]

df_mappings, df_pop = get_data()
popular_movies = df_pop["tmdbId"]
N_pop = np.size(popular_movies)
rand_indices = np.random.choice(np.arange(N_pop), size=N_pop, replace=False)

_tmdb = st.session_state.tmdb
poster_size = "w154"

st.header("Movie Recommender", help="Personalised movie recommendations. Rate at least 5 movies (the more you rate the better the recommendations) and submit to display suggested films.")
with st.container(border=True):
    st.subheader("Rate movies")
    show_ratings()
