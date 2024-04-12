import pandas as pd
import os
import json
import csv
import requests
import os
from os.path import join as joinpath, abspath

from musicbrainz.util import setup_musicbrainz_folders
from utility.utility import is_success_code, send_request_with_wait
from utility.variables import MUSICBRAINZ_ARTISTS_DATA_ITEMS, MUSICBRAINZ_DATA_PATH, MUSICBRAINZ_ITEMS_CSV_NAME

class AliasLoader:

    def __init__(self, endpoint: str):
        self.__endpoint = endpoint
        self.__artists_items_path = MUSICBRAINZ_ARTISTS_DATA_ITEMS
        self.__csv_file_path_test = abspath(joinpath(MUSICBRAINZ_DATA_PATH, f"{endpoint}/artist_names_test.csv"))
        self.__data_path, self.__csv_file_path, self.__items_folder_path = setup_musicbrainz_folders(endpoint)


    # --------------------- LOAD ARTIST NAMES + IDS TO artist_names.csv -------------

    def get_artist_data_from_json(self):
        # List to store the paths of JSON files
        
        artist_data = pd.DataFrame({"ID": [], "Name": []})
        print(artist_data.to_string())

        # Iterate through all files in the folder
        for root, dirs, files in os.walk(self.__artists_items_path):
            for file in files:
                # Check if the file has a .json extension
                if file.endswith('.json'):
                    # Construct the full path to the JSON file
                    json_file_path = os.path.join(root, file)
                    with open(json_file_path, 'r') as f:
                        # Load JSON content
                        content = json.load(f)
                        # Append JSON content to the list
                        print((content["id"], content["name"]))
                        artist_data.loc[len(artist_data)] = (content["id"], content["name"])

        return artist_data
    
    def write_artist_data_to_csv(self):
        print(f"Retrieving artist names from  '{self.__items_folder_path}' ...")
        artist_data = self.get_artist_data_from_json()

        # Write artist names to CSV file
        csv_file_path = os.path.join(self.__csv_file_path)

        print(f"Writing artist names to '{csv_file_path}' ...")
        artist_data.to_csv(csv_file_path, index=False)

        # df2 = pd.read_csv(csv_file_path)
        # print(df2["Names"].to_list())

    # ------------------ GENERATE JSON FILES USING artist_names.csv ----------------    
        
    def get_artist_info(self, artist_name):
        """
        Get artist information from MusicBrainz API for a given artist name.

        Args:
        - artist_name (str): Name of the artist.

        Returns:
        - dict: Artist information retrieved from the MusicBrainz API.
        """
        # MusicBrainz API endpoint
        url = f"https://musicbrainz.org/ws/2/artist"
        # Parameters for the request
        params = {
            "query": artist_name,
            "inc": "aliases",
            "fmt": "json"
        }

        # Make the GET request to MusicBrainz API
        return requests.get(url, params=params)

    def load_aliases_items(self):

        current_path = self.__csv_file_path
        print(f'SCRAPING {self.__endpoint.upper()}')
        # read csv file
        df = self.__update_df_with_fs(current_path)
        # get all uncached items
        uncached_items_df = df[df['CACHED'] == False]
        error_request = set()
        for _, row in uncached_items_df.iterrows():
            id = row['ID']
            name = row['Name']
            item_file_path = joinpath(self.__items_folder_path, f"{id}.json")

            # if we exceed time limit, re scrape after time sleep
            res = self.get_artist_info(name)

            # check if we still have a success code
            if not is_success_code(res.status_code):
                error_request.add(id)
                print(f"Status error code while fetching {id}: {res.status_code}\n{res.json()}")
                continue

            
            # dump playlist data in its own file
            if len(res.json()["artists"]) > 0 and "aliases" in res.json()["artists"][0]:
                with open(item_file_path, "w") as f:
                    input = {"name": res.json()["artists"][0]["name"], 'aliases': res.json()["artists"][0]["aliases"]}
                    json.dump(input, f, indent=2)
                print(f"Successfully scraped {id}: {res.status_code}")
            else:
                error_request.add(id)
                print(f"Skipping {id}: No Aliases Found!")

        df = df[~df['ID'].isin(error_request)]
        df['CACHED'] = True
        df.to_csv(current_path, index=False)
        print(f"\nScraping complete, total ids with error: {len(error_request)}")
        print(f"Removed ids with error request: {'\n'.join(error_request)}")
    
    def __update_df_with_fs(self, csv_file_path):
        # read from csv
        df = pd.read_csv(csv_file_path)
        # drop duplicates
        df.drop_duplicates(subset=['ID'], inplace=True)
        # find all uncached items
        not_cached = set()
        for _, row in df.iterrows():
            if not os.path.exists(joinpath(self.__items_folder_path, f"{row['ID']}.json")):
                not_cached.add(row['ID'])

        df.loc[df['ID'].isin(not_cached), 'CACHED'] = False
        df.loc[~df['ID'].isin(not_cached), 'CACHED'] = True
        df.to_csv(csv_file_path, index=False)
        return df
