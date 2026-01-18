import streamlit as st

st.set_page_config(page_title="Where to Stream", page_icon="🎬")

home = st.Page("pages/page1.py", title="Where to Stream")
reco = st.Page("pages/page2.py", title="Movie Recommender", url_path="movie-recommender")

pg = st.navigation([home, reco])
pg.run()
