import streamlit as st
from src.tmdb import TMDB
from src.st_helpers import display_image_with_hover


def update_media_type():
    media_type = "movie" if st.session_state.is_movie else "tv"
    st.session_state.media_type = media_type


if "is_movie" not in st.session_state:
    st.session_state.is_movie = True

if "media_type" not in st.session_state:
    update_media_type()

if "search" not in st.session_state:
    st.session_state.search = None

if "search_title" not in st.session_state:
    st.session_state.search_title = None

if "services" not in st.session_state:
    st.session_state.services = []

if "country" not in st.session_state:
    st.session_state.country = None

if "trigger_toast" not in st.session_state:
    st.session_state.trigger_toast = False

if "movie_index" not in st.session_state:
    st.session_state.movie_index = 0

if "tmdb" not in st.session_state:
    st.session_state.tmdb = TMDB(media_type="movie")
    locale = st.context.locale
    if locale:
        st.session_state.tmdb.cc = locale.split("-")[1]  # Country code from users browser (regardless of location)
    if not st.session_state.tmdb.API_KEY:
        st.session_state.tmdb.API_KEY = st.secrets.get("TMDB_KEY")


@st.cache_data()
def cache_countries():
    country_dict = _tmdb.get_countries()
    countries = list(country_dict.values())
    countries_sorted = sorted(countries)
    return country_dict, countries, countries_sorted


@st.cache_data()
def cache_services():
    movie_services = _tmdb.get_services()
    tv_services = _tmdb.get_services(False)
    all_movie_services_json, all_tv_services_json = _tmdb.get_all_services()
    all_movie_services = [service["provider_name"].lower() for service in all_movie_services_json]
    all_tv_services = [service["provider_name"].lower() for service in all_tv_services_json]
    return all_movie_services, all_tv_services, all_movie_services_json, all_tv_services_json, sorted(
        list(set(movie_services))), sorted(list(set(tv_services)))


def trigger_toast():
    st.session_state.trigger_toast = True


@st.fragment()
def movie_search():
    with st.container():
        movie_str = "Movie" if st.session_state.is_movie else "TV Show"
        st.text_input(f"Search for {movie_str}", key="search")
        st.toggle("Movie", key="is_movie", on_change=trigger_toast, help="Switch between movies and tv shows.")
        if st.session_state.trigger_toast:
            st.toast(f"Switching to {movie_str}'s")
            st.session_state.trigger_toast = False
        update_media_type()
        submit_button = st.button(label="Submit")
        if submit_button or st.session_state.search:
            _tmdb.get_search(st.session_state.search, st.session_state.media_type)
            display_movie_results(movie_str)


@st.fragment()
def display_movie_results(movie_str):
    if not _tmdb.search_json:
        return
    search_res = _tmdb.search_json["results"]

    def display_func(iii):
        if iii == -1:
            return f"Choose {movie_str}"
        if st.session_state.is_movie:
            return search_res[iii]["title"] \
                + " (" + search_res[iii]["release_date"].split("-")[0] + ")"
        return search_res[iii]["name"] \
            + " (" + search_res[iii]["first_air_date"].split("-")[0] + ")"

    if len(search_res) == 0:
        st.warning(f"No results found for {st.session_state.search}")
    else:
        st.selectbox("Movies", range(-1, len(search_res)), format_func=display_func,
                     label_visibility="collapsed", accept_new_options=False, key="movie_index", index=1)
        if st.session_state.movie_index > -1:
            if st.session_state.is_movie:
                st.session_state.search_title = search_res[st.session_state.movie_index]["title"]
            else:
                st.session_state.search_title = search_res[st.session_state.movie_index]["name"]
            with st.container(border=True):
                display_info(search_res)
            with st.container(border=True):
                display_countries_and_services()


def display_info(search_res):
    st.header(f"{st.session_state.search_title}")
    col1, col2 = st.columns(2)
    with col1:
        size = "original"
        size = "w500"
        st.image(
            f"https://image.tmdb.org/t/p/{size}/{search_res[st.session_state.movie_index]["poster_path"]}.png?api_key={_tmdb.API_KEY}",
            width="stretch")
    with col2:
        st.write(search_res[st.session_state.movie_index]["overview"])
        if not st.session_state.is_movie:
            st.text(f"Country: {", ".join(search_res[st.session_state.movie_index]["origin_country"])}")
        st.write(f"Language: {search_res[st.session_state.movie_index]["original_language"]}")
        if st.session_state.is_movie:
            st.write(f"Release Date: {search_res[st.session_state.movie_index]["release_date"]}")
        else:
            st.write(f"Release Date: {search_res[st.session_state.movie_index]["first_air_date"]}")


@st.fragment()
def display_countries_and_services():
    st.header(f"Where to stream {st.session_state.search_title}")
    _tmdb.get_watch_list(_tmdb.search_json["results"][st.session_state.movie_index]["id"], st.session_state.media_type)
    tab1, tab2 = st.tabs(["Country", "Services"])
    with tab1:
        get_servicies()
    with tab2:
        get_countries()


def get_streaming_options(country_code, type):
    type = type.lower()
    if type == "stream":
        services_flatrate = _tmdb.get_streaming_options(country_code, "flatrate")
        services_free = _tmdb.get_streaming_options(country_code, "free")
        if services_flatrate:
            if services_free:
                services_flatrate.extend(services_free)
            return services_flatrate
        return services_free
    return _tmdb.get_streaming_options(country_code, type)


def get_countries_index():
    if st.session_state.country:
        return countries_sorted.index(st.session_state.country)
    return None


@st.fragment()
def get_servicies():
    tooltip = f"Find all streaming services showing {st.session_state.search} in your country."
    st.selectbox("Select country", countries_sorted, key="country", accept_new_options=False,
                 placeholder="Type country", index=get_countries_index(), help=tooltip)
    country = st.session_state.country
    types = ["Stream", "Buy"]
    if country:
        country_code = list(country_dict.keys())[countries.index(st.session_state.country)]
        for type in types:
            st.subheader(f"{type}: ")
            services = get_streaming_options(country_code, type)
            if services:
                for service in services:
                    display_image_with_hover(
                        f"https://image.tmdb.org/t/p/original/{service['logo_path']}.png?api_key={_tmdb.API_KEY}",
                        service["provider_name"])
            else:
                st.write(f"No services to {type.lower()} {st.session_state.search_title} in {country}.")


def get_country_options(service, type):
    type = type.lower()
    if type == "stream":
        codes_flatrate = _tmdb.get_country_options(provider=service, type="flatrate")
        codes_free = _tmdb.get_country_options(provider=service, type="free")
        if codes_flatrate:
            if codes_free:
                codes_flatrate.extend(codes_free)
            return codes_flatrate
        return codes_free
    return _tmdb.get_country_options(provider=service, type=type)


def get_services_index(services):
    if st.session_state.service:
        return services.index(st.session_state.service)
    return None


@st.fragment()
def get_countries():
    select_services = movie_services if st.session_state.is_movie else tv_services
    tooltip = f"Find all countries showing {st.session_state.search} for your chosen streaming services."
    st.multiselect("Select service", select_services, key="services", accept_new_options=True,
                   placeholder="Type service", default=None, max_selections=4, help=tooltip)
    services = st.session_state.services
    services_json = all_movie_services_json if st.session_state.is_movie else all_tv_services_json
    all_services = all_movie_services if st.session_state.is_movie else all_tv_services
    types = ["Stream", "Buy"]
    if services:
        cols = st.columns(len(services))
        for iii, service in enumerate(services):
            if service.lower() not in all_services:
                st.write(f"Streaming service {service} does not exist.")
                continue
            service_logo_path = services_json[all_services.index(service.lower())]["logo_path"]
            with cols[iii]:
                display_image_with_hover(
                    f"https://image.tmdb.org/t/p/original/{service_logo_path}.png?api_key={_tmdb.API_KEY}", service)
                for type in types:
                    st.subheader(f"{type}: ")
                    codes = get_country_options(service, type)
                    for code in codes:
                        display_image_with_hover(f"https://flagcdn.com/108x81/{code.lower()}.png", country_dict[code])
                    if not codes:
                        st.write(f"No countries to {type.lower()} {st.session_state.search_title} on {service}.")


_tmdb = st.session_state.tmdb
country_dict, countries, countries_sorted = cache_countries()
all_movie_services, all_tv_services, all_movie_services_json, all_tv_services_json, movie_services, tv_services = cache_services()
with st.container(border=True):
    movie_search()

with st.expander("Data Attributions"):
    st.write(
        "This product uses the TMDB API but is not endorsed or certified by [TMDB](%s)." % "https://www.themoviedb.org/")
    st.image("https://upload.wikimedia.org/wikipedia/commons/8/89/Tmdb.new.logo.svg")
    st.write("Streaming availability data provided by [Justwatch](%s)." % "https://www.justwatch.com")
    st.image("https://www.justwatch.com/appassets/img/logo/JustWatch-logo-large.png")