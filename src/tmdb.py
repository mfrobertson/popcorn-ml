import os
from dotenv import load_dotenv
import requests
import sys
import src

class TMDB:

    def __init__(self, media_type="movie", language="en"):
        load_dotenv(f"{src.keys_path}/key.env")
        self.API_KEY = os.getenv("TMDB_KEY")
        self.media_type = media_type
        self.language = language
        self.wl_json = None
        self.search_json = None
        self.countries = None
        self.cc = "US"
        self.movie_services = None
        self.tv_services = None
        self.all_services = None
        self.query = None
        self.tmdb_id = None

    def _check_cache(self, wl=True):
        if wl:
            if self.wl_json is None:
                raise ValueError("wl_json is None")
        elif self.search_json is None:
            raise ValueError("search_json is None")

    def get_countries(self):
        if self.countries is None:
            print("GET countries from TMDB")
            req = requests.get(f"https://api.themoviedb.org/3/configuration/countries?api_key={self.API_KEY}")
            countries = {}
            for country in req.json():
                countries[country["iso_3166_1"]] = country["english_name"]
            self.countries = countries
        return self.countries

    def _get_services(self, req, local=True):
        local_priority = 50
        res = req.json()["results"]
        if local:
            return [service["provider_name"] for service in res if service["display_priorities"].get(self.cc) and service["display_priorities"].get(self.cc) < local_priority]
        return [service["provider_name"].lower() for service in res]

    def get_services(self, movies=True):
        if self.movie_services is None:
            print("GET TV services from TMDB")
            req = requests.get(f"https://api.themoviedb.org/3/watch/providers/movie?api_key={self.API_KEY}")
            self.movie_services = self._get_services(req)
        if self.tv_services is None:
            print("GET TV services from TMDB")
            req = requests.get(f"https://api.themoviedb.org/3/watch/providers/tv?api_key={self.API_KEY}")
            self.tv_services = self._get_services(req)
        if movies:
            return self.movie_services
        return self.tv_services

    def get_all_services(self):
        req = requests.get(f"https://api.themoviedb.org/3/watch/providers/movie?api_key={self.API_KEY}")
        all_movie_services_json = req.json()["results"]
        req = requests.get(f"https://api.themoviedb.org/3/watch/providers/tv?api_key={self.API_KEY}")
        all_tv_services_json = req.json()["results"]
        return all_movie_services_json, all_tv_services_json

    def get_streaming_options(self, country_code="GB", type="flatrate"):
        self._check_cache()
        try:
            res = self.wl_json["results"][country_code][type]
        except KeyError:
            return None
        return res

    def print_streaming_options(self, country_code="GB", type="flatrate"):
        print(f"Printing streaming options for {country_code} of type {type}.")
        res = self.get_streaming_options(country_code=country_code, type=type)
        if not res: print(f"No available streaming options for {type} in {country_code}.")
        for service in res:
            print(service['provider_name'])

    def get_country_options(self, type="flatrate", provider="Netflix"):
        self._check_cache()
        if self.countries is None:
            self.get_countries()
        avail_codes = []
        for country_code in self.countries:
            try:
                res = self.wl_json["results"][country_code][type]
            except KeyError:
                continue
            for service in res:
                if service["provider_name"].lower() == provider.lower():
                    avail_codes.append(country_code)
        return avail_codes

    def print_country_options(self, type="flatrate", provider="Netflix"):
        print(f"Printing country options for {provider} of type {type}.")
        avail_codes = self.get_country_options(type=type, provider=provider)
        for code in avail_codes:
            print(f"{self.countries[code]}")

    def get_watch_list(self, tmdb_id, media_type=None):
        media_type = media_type or self.media_type
        if tmdb_id == self.tmdb_id and media_type == self.media_type:
            return self.wl_json
        print(f"GET watch_list from TMDB for {media_type} id: {tmdb_id}")
        req = requests.get(f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers?api_key={self.API_KEY}")
        self.wl_json = req.json()
        self.tmdb_id = tmdb_id
        return self.wl_json

    def get_search(self, query, media_type=None, language=None):
        media_type = media_type or self.media_type
        language = language or self.language
        if query == self.query and media_type == self.media_type and language == self.language:
            return self.search_json
        print(f"GET results from TMDB for query: {query}")
        req = requests.get(f"https://api.themoviedb.org/3/search/{media_type}?api_key={self.API_KEY}&query={query}&language={language}")
        self.search_json = req.json()
        self.media_type = media_type
        self.language = language
        self.query = query
        return self.search_json

    def print_search_results(self):
        self._check_cache(False)
        for res in self.search_json["results"]:
            try:
                print(res["id"], res["title"], res["media_type"])
            except KeyError:
                print(res["id"], res["name"], res["media_type"])

    def get_details_by_id(self, tmdb_id, media_type, language="en"):
        req = requests.get(f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}?api_key={self.API_KEY}&language={language}")
        json = req.json()
        return json


if __name__ == "__main__":

    tmdb = TMDB(media_type="multi")

    input_args = sys.argv
    if len(input_args) > 1:
        for arg in input_args[1:]:
            tmdb.get_search(arg, "multi")
            tmdb.print_search_results()
            tmdb_id = tmdb.search_json["results"][0]["id"]
            media_type = tmdb.search_json["results"][0]["media_type"]
            tmdb.get_watch_list(tmdb_id, media_type)
            tmdb.print_country_options(type="flatrate", provider="Disney Plus")
            tmdb.print_streaming_options()

    print("Exit")