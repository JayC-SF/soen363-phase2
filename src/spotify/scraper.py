import string
from typing import Iterable
from spotify.util import setup_spotify_folders
from utility.auth_token import SPOTIFY_AUTH_TOKEN
from utility.utility import is_success_code, send_request_with_wait
import pandas as pd
import requests
from requests import Response
import os
from os.path import join as joinpath
import json
from pathlib import Path
import random
import time
import random

from utility.variables import DATA_PATH, SLEEP_TIMER_AVG, SPOTIFY_API_URL

# sample curl request of a playlist
"""
curl --request GET \
  --url https://api.spotify.com/v1/playlists/3cEYpjA9oz9GiPac4AsH4n \
  --header 'Authorization: Bearer 1POdFZRZbvb...qqillRxMr2z'
"""


class SpotifyScraper:

    def __init__(self, endpoint: str):
        self.__endpoint = endpoint
        self.__data_path, self.__csv_file_path, self.__items_folder_path, _ = setup_spotify_folders(endpoint)

    def scrape_items(self, batchmode: int = 1):
        """Scrapes items using ids defined in csv file

        Args:
            batchmode (bool, optional): Performs batchmode scraping request. Defaults to False.
        """
        if batchmode > 1:
            self.scrape_batch_items(batchmode)
        else:
            self.scrape_nonbatch_items()

    def __update_df_with_fs(self):
        # read from csv
        df = pd.read_csv(self.__csv_file_path)
        # drop duplicates
        df.drop_duplicates(subset=['ID'], inplace=True)
        # find all uncached items
        # not_cached = set()
        # for _, row in df.iterrows():
        #     if not os.path.exists(joinpath(self.__items_folder_path, f"{row['ID']}.json")):
        #         not_cached.add(row['ID'])
        filtered_files = filter(lambda f: f.endswith(".json"), os.listdir(self.__items_folder_path))
        mapper = map(lambda id: id.replace(".json", ""), filtered_files)
        ids = set(mapper)
        df.loc[~df['ID'].isin(ids), 'CACHED'] = False
        df.loc[df['ID'].isin(ids), 'CACHED'] = True
        df.to_csv(self.__csv_file_path, index=False)
        return df

    def scrape_nonbatch_items(self):
        """Scrapes items without using batchmode requests. Performs a request for each ids in the csv file.
        The function stores the output of the request in a json file.
        The function skips all ids that are cached.

        Raises:
            Exception: An exception is the code is neither 429 or a success code.
        """
        timer = 0

        print(f'SCRAPING {self.__endpoint.upper()}')
        # read csv file
        df = self.__update_df_with_fs()
        # get all uncached items
        uncached_items_df = df[df['CACHED'] == False]
        error_request = set()
        for _, row in uncached_items_df.iterrows():
            id = row['ID']
            item_file_path = joinpath(self.__items_folder_path, f"{id}.json")

            print(f"Running timer of {timer} seconds ...")
            time.sleep(timer)

            # if we exceed time limit, re scrape after time sleep
            res = send_request_with_wait(SpotifyScraper.scrape_single_id, self, id)

            # gives a random time to sleep between 0 and SLEEP_TIMER_AVG
            # timer = random.random()*SLEEP_TIMER_AVG*2
            timer = SLEEP_TIMER_AVG

            # check if we still have a success code
            if not is_success_code(res.status_code):
                error_request.add(id)
                print(f"Status error code while fetching {id}: {res.status_code}\n{res.json()}")
                continue

            print(f"Successfully scraped {id}: {res.status_code}")
            # dump playlist data in its own file
            with open(item_file_path, "w") as f:
                json.dump(res.json(), f, indent=2)

        df = df[~df['ID'].isin(error_request)]
        df['CACHED'] = True
        df.to_csv(self.__csv_file_path, index=False)
        print(f"\nScraping complete, total ids with error: {len(error_request)}")
        print(f"Removed ids with error request: {'\n'.join(error_request)}")

    def scrape_single_id(self, id: str) -> Response:
        """Scrapes the response of a single id for an item

        Args:
            id (str): Id of the item to be scraped.

        Returns:
            Response: Response object from `requests` library
        """
        URL = f"{SPOTIFY_API_URL}/{self.__endpoint}/{id}"
        headers = {
            'Authorization': f"{SPOTIFY_AUTH_TOKEN.get_authorization()}"
        }
        res = requests.get(url=URL, headers=headers)
        return res

    def scrape_batch_items(self, batchmode):
        """_summary_

        Raises:
            Exception: _description_
        """
        print(f'SCRAPING {self.__endpoint.upper()}')
        # read csv file
        df = self.__update_df_with_fs()
        # get the uncached items
        uncached_items_df = df[df['CACHED'] == False].copy()
        error_request = set()
        timer = 0

        while (len(uncached_items_df) != 0):
            # get the batch of ids to send in request
            batch_items_df = uncached_items_df[:batchmode]
            batch_ids = set(batch_items_df['ID'].to_list())
            uncached_items_df.drop(batch_items_df.index, inplace=True)

            # send the batch request and check status codes
            print(f"Running timer of {timer} seconds ...")
            time.sleep(timer)

            res = send_request_with_wait(SpotifyScraper.scrape_batch_ids, self, batch_ids)

            # timer = random.random()*SLEEP_TIMER_AVG*2
            timer = SLEEP_TIMER_AVG
            # send request without batch, by sending a request 1 by 1
            if not is_success_code(res.status_code):
                print(f"Status error code {res.status_code} while fetching:\n{str(res.content)}")
                print("Fetching in non batchmode for this batch")
                items = {
                    self.__endpoint: []
                }
                for batch_id in batch_ids:

                    print(f"Running timer of {timer} seconds ...")
                    time.sleep(timer)

                    res = send_request_with_wait(SpotifyScraper.scrape_single_id, self, batch_id)

                    # Set the timer for the next round
                    # timer = random.random()*SLEEP_TIMER_AVG*2
                    timer = SLEEP_TIMER_AVG

                    if not is_success_code(res.status_code):
                        print(f"Status error code while fetching {batch_id}: {res.status_code}\n{res.json()}")
                        continue
                    items[self.__endpoint].append(res.json())
            else:
                # get the json response
                items = res.json()

            # store each scraped item in its folder.
            for item in items[self.__endpoint]:
                # skip the null ones
                if (item is None):
                    continue
                id = item['id']
                item_file_path = joinpath(self.__items_folder_path, f"{id}.json")
                print(f"Successfully scraped {id}: {res.status_code}.")
                # dump playlist data in its own file
                with open(item_file_path, "w") as f:
                    json.dump(item, f, indent=2)
                if id in batch_ids:
                    batch_ids.remove(id)
            # log all missing ids from that batch request
            for missing_id in batch_ids:
                f"Response is missing {missing_id} in batch request."

            error_request.union(batch_ids)

        df = df[~df['ID'].isin(error_request)]
        df['CACHED'] = True
        df.to_csv(self.__csv_file_path, index=False)
        print(f"\nScraping complete, total ids with error: {len(error_request)}")
        print(f"Removed ids with error request: {'\n'.join(error_request)}")

    def scrape_batch_ids(self, ids: Iterable[str]):
        """_summary_
        """
        if (len(ids) <= 0):
            raise Exception(f"Cannot send 0 ids in {self.__endpoint} batch request.")
        PARAMS = {
            "ids": ",".join(ids)
        }
        URL = f"{SPOTIFY_API_URL}/{self.__endpoint}"
        headers = {
            'Authorization': f"{SPOTIFY_AUTH_TOKEN.get_authorization()}"
        }
        res = requests.get(url=URL, headers=headers, params=PARAMS)
        return res

    def generate_random_string(self, length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))

    def generate_playlist_ids(self):
        print("PRINTING LIST OF PLAYLIST IDS:")

        # Spotify API endpoint
        # URL = "https://api.spotify.com/v1/browse/featured-playlists?offset=99&limit=50"
        URL = "https://api.spotify.com/v1/browse/categories/blues/playlists?offset=50&limit=50"
        playlist_list = []
        headers = {
            'Authorization': f"{SPOTIFY_AUTH_TOKEN.get_authorization()}"
        }
        # Make a GET request to Spotify API
        response = requests.get(URL, headers=headers)

        if response.status_code == 200:
            # Extract playlist IDs from the response
            playlists = response.json()["playlists"]["items"]
            for playlist in playlists:
                uri = playlist["uri"]
                if uri is not None:
                    clean_uri = uri.replace('spotify:playlist:', '')
                    playlist_list.append(clean_uri)

            # Print the list of playlist IDs
            print("List of Playlist IDs:")
            for playlist_id in playlist_list:
                print(playlist_id)
        else:
            print(response.json())

    def generate_artists_ids(self):
        print("PRINTING LIST OF ARTIST IDS:")

        # Spotify API endpoint
        URL = "https://api.spotify.com/v1/search"

        # Search query for artists
        search_query = "artist:"

        # Parameters for the request
        params = {
            "q": search_query,
            "type": "artist",
            "limit": 40,  # Adjust the limit as per your requirement
            "market": "US",  # Specify the market for better results
            "include_external": "audio",  # Include external content like popularity metrics
            "offset": 0,  # Offset for pagination, if needed
            "sort": "popularity"  # Sort by popularity
        }

        headers = {
            'Authorization': f"{SPOTIFY_AUTH_TOKEN.get_authorization()}"
        }
        # Make a GET request to Spotify API
        response = requests.get(URL, params=params, headers=headers)

        if response.status_code == 200:
            # Extract artist IDs from the response
            artists = response.json()["artists"]["items"]
            artist_ids = [artist["id"] for artist in artists]

            # Print the list of artist IDs
            print("List of Artist IDs:")
            for artist_id in artist_ids:
                print(artist_id)
        else:
            print(response.json())
