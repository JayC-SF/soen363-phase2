
from pathlib import Path
from spotify.util import setup_spotify_folders
import pandas as pd
import os
from os.path import abspath, join as joinpath, exists
import json
from utility.variables import SPOTIFY_DATA_PATH, SPOTIFY_ITEMS_CSV_NAME
from tqdm import tqdm


def load_info_from_playlists():
    _, tracks_csv, _, _ = setup_spotify_folders('tracks')
    _, playlists_csv, playlist_items_folder, _ = setup_spotify_folders('playlists')
    _, albums_csv, _, _ = setup_spotify_folders('albums')
    _, artists_csv, _, _ = setup_spotify_folders('artists')
    print("Loading playlists...")
    playlists_df = pd.read_csv(playlists_csv)
    print("Drop duplicates of playlists...")
    playlists_df.drop_duplicates(subset=['ID'], inplace=True)
    print("Removing playlists that are not cached...")
    playlists_df = playlists_df[playlists_df['CACHED'] == True]
    playlist_ids = set(playlists_df['ID'].to_list())
    del playlists_df
    total_playlist_ids = len(playlist_ids)
    print(f"Total playlists to read: {total_playlist_ids}")
    tracks = set()
    artists = set()
    albums = set()
    # print new line for the progress bar
    with tqdm(total=total_playlist_ids) as pbar:
        for playlist_id in playlist_ids:
            load_info_from_playlist(playlist_id, playlist_items_folder, tracks, artists, albums)
            pbar.update(1)

    del playlist_ids
    tracks_size_diff = output_to_csv(tracks_csv, tracks, "tracks")
    del tracks
    artists_size_diff = output_to_csv(artists_csv, artists, "artists")
    del artists
    albums_size_diff = output_to_csv(albums_csv, albums, "albums")
    del albums

    print(f"Added {tracks_size_diff} new tracks from playlists.")
    print(f"Added {artists_size_diff} new albums from playlists.")
    print(f"Added {albums_size_diff} new artists from playlists.")


def output_to_csv(csv_path: str, ids: set[str], type: str):
    print(f"Load {type} csv to remove duplicates...")
    csv_id = set(pd.read_csv(csv_path)['ID'].to_list())
    ids.difference_update(csv_id)
    # delete csv_id's reference for garbage collector
    del csv_id
    # map ids to csv format
    ids = [f"{id},False\n" for id in ids if id is not None]
    size = len(ids)
    if size:
        print(f"Writing to {type} csv...")
        with open(csv_path, "a") as f:
            f.writelines(ids)
    else:
        print(f"Nothing to write for {type}")
    return size


def load_info_from_playlist(playlist_id, playlist_items_folder, tracks_set: set[str], artists_set: set[str], albums_set: set[str]):
    # check if the playlist has been scraped
    playlist_json_file = os.path.join(playlist_items_folder, f"{playlist_id}.json")
    if not os.path.exists(playlist_json_file):
        print(f"Playlist {playlist_id} needs to be scraped before loading tracks")
        return

    with open(playlist_json_file, "r") as f:
        playlist_json = json.load(f)

    # find all the tracks
    tracks = playlist_json['tracks']['items']
    for track in tracks:

        track = track['track']
        if track is None:
            continue
        # add new track in set
        tracks_set.add(track['id'])
        albums_set.add(track['album']['id'])
        # add all artists from albums
        for artist in track['album']['artists']:
            artists_set.add(artist['id'])
        # add artists in track
        for artist in track['artists']:
            artists_set.add(artist['id'])


def load_authors_from_audiobooks():
    _, _, items_audiobooks_folder_path, _ = setup_spotify_folders('audiobooks')
    _, _, items_authors_folder_path, _ = setup_spotify_folders('authors')

    if not os.path.exists(items_audiobooks_folder_path):
        print(f"No directory was found at {items_audiobooks_folder_path}")
        return

    files = [f for f in os.listdir(items_audiobooks_folder_path) if
             os.path.isfile(os.path.join(items_audiobooks_folder_path, f)) and f.endswith('.json')]

    author_files = [f for f in os.listdir(items_authors_folder_path) if
                    os.path.isfile(os.path.join(items_authors_folder_path, f)) and f.endswith('.json')]
    authors_size = len(author_files)

    if not files:
        print(f"No data was found at {items_audiobooks_folder_path}")
        return
    for file in files:
        file_path = os.path.join(items_audiobooks_folder_path, file)
        with open(file_path, 'r') as f:
            load_authors_from_single_audiobook(f, file_path, items_authors_folder_path)

    author_files = [f for f in os.listdir(items_authors_folder_path) if
                    os.path.isfile(os.path.join(items_authors_folder_path, f)) and f.endswith('.json')]
    print(f"Added {len(author_files) - authors_size} new audiobooks with authors.")


def load_authors_from_single_audiobook(file, file_path, items_authors_folder_path):
    data = json.load(file)
    if data:
        audiobook_id = data['id']
        authors = data['authors']

        author = {'authors': authors, 'audiobook_id': audiobook_id}
        if os.path.isfile(f'{items_authors_folder_path}/{author['audiobook_id']}.json'):
            return
        with open(f'{items_authors_folder_path}/{author['audiobook_id']}.json', 'w') as f:
            json.dump(author, f, ensure_ascii=False, indent=2)
    else:
        print(f"Warning: Empty JSON file skipped - {file_path}")


def load_chapters_from_audiobooks():
    _, _, items_audiobooks_folder_path, _ = setup_spotify_folders('audiobooks')
    _, _, items_chapters_folder_path, _ = setup_spotify_folders('chapters')

    if not os.path.exists(items_audiobooks_folder_path):
        print(f"No directory was found at {items_audiobooks_folder_path}")
        return

    files = [f for f in os.listdir(items_audiobooks_folder_path) if
             os.path.isfile(os.path.join(items_audiobooks_folder_path, f)) and f.endswith('.json')]

    chapter_files = [f for f in os.listdir(items_chapters_folder_path) if
                     os.path.isfile(os.path.join(items_chapters_folder_path, f)) and f.endswith('.json')]
    chapters_size = len(chapter_files)

    if not files:
        print(f"No data was found at {items_audiobooks_folder_path}")
        return
    for file in files:
        file_path = os.path.join(items_audiobooks_folder_path, file)
        with open(file_path, 'r') as f:
            load_chapters_from_single_audiobook(f, file_path, items_chapters_folder_path)

    chapter_files = [f for f in os.listdir(items_chapters_folder_path) if
                     os.path.isfile(os.path.join(items_chapters_folder_path, f)) and f.endswith('.json')]
    print(f"Added {len(chapter_files) - chapters_size} new chapters.")


def load_chapters_from_single_audiobook(file, file_path, items_chapters_folder_path):
    data = json.load(file)
    if data:
        chapter_items = data['chapters']['items']
        audiobook = data.copy()
        del audiobook['chapters']

        for chapter in chapter_items:
            chapter['audiobook'] = audiobook
            if os.path.isfile(f'{items_chapters_folder_path}/{chapter['id']}.json'):
                return
            with open(f'{items_chapters_folder_path}/{chapter['id']}.json', 'w') as f:
                json.dump(chapter, f, ensure_ascii=False, indent=2)
    else:
        print(f"Warning: Empty JSON file skipped - {file_path}")


def load_genres():
    endpoint = 'genres'
    data_path = abspath(joinpath(SPOTIFY_DATA_PATH, endpoint))
    csv_genres_path = abspath(joinpath(data_path, 'genres.csv'))

    # create data folder if they don't exist
    Path(data_path).mkdir(parents=True, exist_ok=True)
    if not exists(csv_genres_path):
        with open(csv_genres_path, "w") as f:
            f.write('GENRES')
        # log info
        print(f"No data currently stored for {endpoint}, successfully created folders and csv file.")

    # get paths for artists and albums
    _, artists_csv, artists_items_folder, _ = setup_spotify_folders('artists')
    _, albums_csv, albums_items_folder, _ = setup_spotify_folders('albums')
    artists_df = pd.read_csv(artists_csv)
    albums_df = pd.read_csv(albums_csv)
    genres_df = pd.read_csv(csv_genres_path)
    genres_count = len(genres_df)

    # keep only the ones that are cached
    artists_df = artists_df[artists_df['CACHED']]
    albums_df = albums_df[albums_df['CACHED']]

    for _, row in artists_df.iterrows():
        artist_id = row['ID']
        artist_json_file = os.path.join(artists_items_folder, f"{artist_id}.json")
        with open(artist_json_file, "r") as f:
            artist_json = json.load(f)
        # combine the genres from artists
        for g in artist_json['genres']:
            genres_df.loc[len(genres_df)] = [g]

    for _, row in albums_df.iterrows():
        album_id = row['ID']
        album_json_file = os.path.join(albums_items_folder, f"{album_id}.json")
        with open(album_json_file, "r") as f:
            album_json = json.load(f)
        # combine the genres from album
        for g in album_json['genres']:
            genres_df.loc[len(genres_df)] = [g]

    genres_df.drop_duplicates(subset=['GENRES'], inplace=True)
    genres_df.to_csv(csv_genres_path, index=False)
    print(f"Total number of genres added: {len(genres_df)-genres_count}")
