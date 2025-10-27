import os
from dotenv import load_dotenv
import requests
import sys


class TMDB:

    def __init__(self, media_type="movie", language="en"):
        load_dotenv("../src/key.env")
        self.API_KEY = os.getenv("TMDB_KEY")
        self.media_type = media_type
        self.language = language
        self.wl_json = None
        self.search_json = None
        self.countries = None

    def check_cache(self, wl=True):
        if wl:
            if self.wl_json is None:
                raise ValueError("wl_json is None")
        if self.search_json is None:
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

    def print_streaming_options(self, country_code="GB", type="flatrate"):
        self.check_cache()
        print(f"Printing streaming options for {country_code} of type {type}.")
        try:
            res = self.wl_json["results"][country_code][type]
        except KeyError:
            print(f"No available streaming options for {type} in {country_code}.")
            return
        for service in res:
            print(service['provider_name'])

    def print_country_options(self, type="flatrate", provider="Netflix"):
        self.check_cache()
        print(f"Printing country options for {provider} of type {type}.")
        if self.countries is None:
            self.get_countries()
        for country_code in self.countries:
            try:
                res = self.wl_json["results"][country_code][type]
            except KeyError:
                continue
            for service in res:
                if service["provider_name"] == provider:
                    print(f"{self.countries[country_code]}")

    def get_watch_list(self, tmdb_id, media_type=None):
        media_type = media_type or self.media_type
        print(f"GET watch_list from TMDB for {media_type} id: {tmdb_id}")
        req = requests.get(f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/watch/providers?api_key={self.API_KEY}")
        self.wl_json = req.json()
        print(self.wl_json)
        return self.wl_json

    def get_search(self, query, media_type=None, language=None):
        print(f"GET results from TMDB for query: {query}")
        media_type = media_type or self.media_type
        language = language or self.language
        req = requests.get(f"https://api.themoviedb.org/3/search/{media_type}?api_key={self.API_KEY}&query={query}&language={language}")
        self.search_json = req.json()
        return self.search_json

    def print_search_results(self):
        self.check_cache(False)
        for res in self.search_json["results"]:
            try:
                print(res["id"], res["title"], res["media_type"])
            except KeyError:
                print(res["id"], res["name"], res["media_type"])


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