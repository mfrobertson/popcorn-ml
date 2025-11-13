import streamlit as st

from tmdb import TMDB

if "tmdb" not in st.session_state:
    st.session_state.tmdb = TMDB(media_type="movie")

def update_media_type():
    media_type = "movie" if st.session_state.is_movie else "tv"
    st.session_state.media_type = media_type
    st.session_state.tmdb.media_type = media_type

if "is_movie" not in st.session_state:
    st.session_state.is_movie = True

if "media_type" not in st.session_state:
    update_media_type()

if "search" not in st.session_state:
    st.session_state.search = None

if "service" not in st.session_state:
    st.session_state.service = None

if "country" not in st.session_state:
    st.session_state.country = None

def display_image_with_hover(image_url, hover_text):
    html = f"""
    <style>
    .tooltip {{
      position: relative;
      display: inline-block;
      cursor: pointer;
    }}
    
    .tooltip .tooltiptext {{
      visibility: hidden;
      width: 120px;
      background-color: black;
      color: white;
      text-align: center;
      border-radius: 6px;
      padding: 5px 0;
      position: absolute;
      z-index: 1;
      top: 0%;
      left: 50%;
      margin-left: -60px;
      opacity: 0;
      transition: opacity 0.3s;
    }}
    
    .tooltip:hover .tooltiptext {{
      visibility: visible;
      opacity: 1;
    }}
    
    .tooltip img {{
      border-radius: 8px;             
      box-shadow: 0 2px 5px rgba(0,0,0,0.15);
      transition: transform 0.2s;
    }}
    
    .tooltip img:hover {{
      transform: scale(1.05);       
    }}

    </style>
    <div class='tooltip'>
      <img src='{image_url}' alt='Image' style='margin-bottom:10px;'>
      <span class='tooltiptext'>{hover_text}</span>
    </div>
    """
    return st.markdown(html, unsafe_allow_html=True)

@st.cache_data()
def cache_countries():
    country_dict = _tmdb.get_countries()
    countries = list(country_dict.values())
    countries_sorted = sorted(countries)
    return country_dict, countries, countries_sorted

@st.cache_data()
def cache_service():
    movie_services = _tmdb.get_services()
    tv_services = _tmdb.get_services(False)
    return set(movie_services), set(tv_services)

@ st.fragment()
def movie_search():
    with st.container():
        movie_str = "Movie" if st.session_state.is_movie else "TV Show"
        st.text_input(f"Search for {movie_str}", key="search")
        st.toggle("Movie", key="is_movie")
        update_media_type()
        submit_button = st.button(label="Submit")
        if submit_button or st.session_state.search:
            _tmdb.get_search(st.session_state.search)
            display_movie_results(movie_str)

@st.fragment()
def display_movie_results(movie_str):
    with st.container(border=True):
        search_res = _tmdb.search_json["results"]
        def display_func(iii):
            if iii == -1:
                return f"Choose {movie_str}"
            if st.session_state.is_movie:
                return search_res[iii]["title"] \
                + " ("  + search_res[iii]["release_date"].split("-")[0] + ")"
            return search_res[iii]["name"] \
                + " ("  + search_res[iii]["first_air_date"].split("-")[0] + ")"

        st.selectbox("Movies", range(-1, len(search_res)), format_func=display_func,
                     label_visibility="collapsed", accept_new_options=False, key="movie_index")
        if st.session_state.movie_index > -1:
            display_info(search_res)
            display_countries_and_services()

def display_info(search_res):
    col1, col2 = st.columns(2)
    with col1:
        size = "original"
        size = "w500"
        st.image(
            f"https://image.tmdb.org/t/p/{size}/{search_res[st.session_state.movie_index]["poster_path"]}.png?api_key={_tmdb.API_KEY}", use_container_width=True)
    with col2:
        st.write(search_res[st.session_state.movie_index]["overview"])
        if not st.session_state.is_movie:
            st.text(f"Country: {", ".join(search_res[st.session_state.movie_index]["origin_country"])}")
        st.write(f"Language: {search_res[st.session_state.movie_index]["original_language"]}")


@st.fragment()
def display_countries_and_services():
    _tmdb.get_watch_list(_tmdb.search_json["results"][st.session_state.movie_index]["id"])
    col1, col2 = st.columns(2)
    with col1:
        get_servicies()
    with col2:
        get_countries()

def get_streaming_options(country_code, type="subscription"):
    if type == "subscription":
        services_flatrate = _tmdb.get_streaming_options(country_code, "flatrate")
        services_free = _tmdb.get_streaming_options(country_code, "free")
        if services_flatrate:
            if services_free:
                services_flatrate.extend(services_free)
            return services_flatrate
        return services_free
    return _tmdb.get_streaming_options(country_code, type)

@st.fragment()
def get_servicies():
    st.selectbox("Select country", countries_sorted, key="country", accept_new_options=False, placeholder="Select country")
    country = st.session_state.country
    if country:
        country_code = list(country_dict.keys())[countries.index(st.session_state.country)]
        services = get_streaming_options(country_code)
        if services:
            for service in services:
                display_image_with_hover(f"https://image.tmdb.org/t/p/original/{service['logo_path']}.png?api_key={_tmdb.API_KEY}", service["provider_name"])

def get_country_options(service, type="subscription"):
    if type == "subscription":
        codes_flatrate = _tmdb.get_country_options(provider=service, type="flatrate")
        codes_free = _tmdb.get_country_options(provider=service, type="free")
        if codes_flatrate:
            if codes_free:
                codes_flatrate.extend(codes_free)
            return codes_flatrate
        return codes_free
    return _tmdb.get_country_options(provider=service, type=type)

@st.fragment()
def get_countries():
    services = movie_services if st.session_state.is_movie else tv_services
    st.selectbox("Select service", services, key="service", accept_new_options=False, placeholder="Select service")
    service = st.session_state.service
    if service:
        codes = get_country_options(service)
        for code in codes:
            display_image_with_hover(f"https://flagcdn.com/40x30/{code.lower()}.png", country_dict[code])


_tmdb = st.session_state.tmdb
country_dict, countries, countries_sorted = cache_countries()
movie_services, tv_services = cache_service()
with st.container(border=True):
    movie_search()
