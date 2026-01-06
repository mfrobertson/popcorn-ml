import streamlit as st
from src.tmdb import TMDB


Ncol = 5
Nrow = 20

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

@st.fragment()
def rating_slider(col_idx):
    rating_values = [i for i in range(0,11)]
    st.select_slider("Rating", rating_values, key=f"slider_{col_idx}", label_visibility="collapsed")

@st.fragment()
def ratings():
    cols = st.columns(Ncol)
    for iii, col in enumerate(cols):
        with col:
            col_idx = iii + (st.session_state.row * Ncol)
            st.image(f"https://image.tmdb.org/t/p/{poster_size}/{poster_path}.png?api_key={_tmdb.API_KEY}")
            rating_slider(col_idx)

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

_tmdb = st.session_state.tmdb
poster_size = "w154"
movie_json = _tmdb.get_details_by_id(603, "movie")
poster_path = movie_json["poster_path"]

st.header("Movie Recommender", help="Personalised movie recommendations. Rate at least 5 movies (the more you rate the better the recommendations) and submit to get recommendations.")
with st.container(border=True):
    st.subheader("Rate movies")
    show_ratings()
