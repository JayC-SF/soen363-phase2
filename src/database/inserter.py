import json
import threading
import time
from queue import Queue
from typing import List
from os.path import abspath, join as joinpath

import mysql.connector
import pandas as pd

from spotify.models.album_model import AlbumModel
from spotify.models.alias_model import AliasModel
from spotify.models.artist_model import ArtistModel
from spotify.models.audiobook_model import AudiobookModel
from spotify.models.author_model import AuthorModel
from spotify.models.chapter_model import ChapterModel
from spotify.models.playlist_model import PlaylistModel
from spotify.models.track_model import TrackModel
from spotify.parser import SpotifyParser
from spotify.util import setup_spotify_folders
from utility.variables import DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME, MUSICBRAINZ_DATA_PATH, SPOTIFY_DATA_PATH
from tqdm import tqdm
import os
import math

class DatabaseInserter:
    def __init__(self, data_type: str):
        self.__data_type = data_type
        self.__db = mysql.connector.connect(
            host=DATABASE_HOST,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
            database=DATABASE_NAME
        )

    def insert_data(self):
        match self.__data_type:
            case 'genres':
                self.__insert_genres()
            case 'albums':
                self.__insert_albums()
            case 'artists':
                self.__insert_artists()
            case 'chapters':
                self.__insert_chapters_genres()
            case 'playlists':
                self.__insert_playlists()
            case 'tracks':
                self.__insert_tracks()
            case 'audiobooks':
                self.__insert_audiobooks()
            case 'authors':
                self.__insert_authors()
            case 'aliases':
                self.__insert_aliases()
            case 'tracks_artists':
                self.__insert_tracks_artists()
            # case 'artists_aliases':
                # self.__insert_artists_aliases()
            case 'tracks_albums':
                self.__insert_tracks_albums()
            case 'artists_genres':
                self.__insert_artists_genres()
            case 'available_markets_albums':
                self.__insert_available_markets_albums()
            case 'available_markets_tracks':
                self.__insert_available_markets_tracks()
            case 'playlists_tracks':
                self.__insert_playlists_tracks()
            case 'audiobooks_authors':
                self.__insert_audiobooks_authors()
            case 'audiobooks_chapters':
                self.__insert_audiobooks_chapters()
            case _:
                print(f"Error: The function to insert data for {self.__data_type} has not been implemented yet.")

    def __insert_genres(self):
        cursor = self.__db.cursor()
        data_path = abspath(joinpath(SPOTIFY_DATA_PATH, 'genres'))
        csv_genres_path = abspath(joinpath(data_path, 'genres.csv'))
        df = pd.read_csv(csv_genres_path)

        start_time = time.time()
        num_genres = 0

        for _, row in df.iterrows():
            genre_name = row['GENRES']
            check_query = "SELECT EXISTS(SELECT 1 FROM Genre WHERE genre_name = %s)"
            cursor.execute(check_query, (genre_name,))
            exists = cursor.fetchone()[0]
            if not exists:
                insert_query = """
                            INSERT INTO Genre (genre_name)
                            VALUES (%s)
                            """
                cursor.execute(insert_query, (genre_name,))
                self.__db.commit()
                print(f"Inserted genre data for {genre_name}")
                num_genres += 1
            else:
                print(f"Row already exists for {genre_name}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_genres} albums in {end_time - start_time} seconds")

    def __insert_albums(self):
        parser = SpotifyParser('albums', AlbumModel)
        albums: List[AlbumModel] = parser.parse_all()
        cursor = self.__db.cursor()

        start_time = time.time()
        num_albums = 0

        for album in albums:
            check_query = "SELECT EXISTS(SELECT 1 FROM Album WHERE spotify_id = %s)"
            cursor.execute(check_query, (album.spotify_id,))
            exists = cursor.fetchone()[0]
            if not exists:
                # If the album does not exist, insert it
                insert_query = """
                            INSERT INTO Album (album_name, spotify_id, total_tracks, popularity, release_date, label, external_url, href, type)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                album_data = (
                    album.album_name, album.spotify_id, album.total_tracks, album.popularity, album.release_date,
                    album.label, album.external_url, album.href, album.type
                )
                cursor.execute(insert_query, album_data)
                self.__db.commit()
                print(f"Inserted album data for {album.spotify_id}")
                num_albums += 1
            # else:
                # print(f"Row already exists for {album.spotify_id, album.album_name}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_albums} albums in {end_time - start_time} seconds")

    def __insert_artists(self):
        parser = SpotifyParser('artists', ArtistModel)
        artists: List[ArtistModel] = parser.parse_all()
        cursor = self.__db.cursor()

        start_time = time.time()
        num_artists = 0

        for artist in artists:
            check_query = "SELECT EXISTS(SELECT 1 FROM Artist WHERE spotify_id = %s)"
            cursor.execute(check_query, (artist.spotify_id,))
            exists = cursor.fetchone()[0]
            if not exists:
                insert_query = """
                            INSERT INTO Artist (spotify_id, artist_name, nb_followers, popularity, external_url, href, uri)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                artist_data = (
                    artist.spotify_id, artist.artist_name, artist.nb_followers, artist.popularity, artist.external_url,
                    artist.href, artist.uri
                )
                cursor.execute(insert_query, artist_data)
                self.__db.commit()
                print(f"Inserted artist data for {artist.spotify_id}")
                num_artists += 1
            else:
                print(f"Row already exists for {artist.spotify_id, artist.artist_name}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_artists} artists in {end_time - start_time} seconds")

    def __insert_artists_genres(self):
        parser = SpotifyParser('artists', ArtistModel)
        artists: List[ArtistModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'genres', json_data.get('genres')) or mapped_object
            )
        )
        cursor = self.__db.cursor()
        start_time = time.time()
        num_inserts = 0

        for artist in artists:
            cursor.execute("SELECT artist_id FROM Artist WHERE spotify_id = %s", (artist.spotify_id,))
            artist_result = cursor.fetchone()
            if artist_result:
                artist_id = artist_result[0]
            else:
                continue

            for genre_name in artist.genres:
                cursor.execute("SELECT genre_id FROM Genre WHERE genre_name = %s", (genre_name,))
                genre_result = cursor.fetchone()
                if genre_result:
                    genre_id = genre_result[0]
                else:
                    continue

                check_query = "SELECT COUNT(1) FROM Artists_Genres WHERE artist_id = %s AND genre_id = %s"
                cursor.execute(check_query, (artist_id, genre_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute(
                        "INSERT INTO Artists_Genres (artist_id, genre_id) VALUES (%s, %s)",
                        (artist_id, genre_id)
                    )
                    self.__db.commit()
                    num_inserts += 1
                    print(f"Inserted artist and genre data for artist_id: {artist.spotify_id}, genre_id: {genre_id}")
                else:
                    print(f"Row already exists for artist_id: {artist.spotify_id}, genre_id: {genre_id}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} artists and genres in {end_time - start_time} seconds")

    def __insert_aliases(self):
        data_path, csv_file_path, items_folder_path, mapper_file_path = setup_spotify_folders("aliases", source_data_path=MUSICBRAINZ_DATA_PATH, create_ids_csv=False)
        artists_spot_id = [f.replace(".json", "") for f in os.listdir(items_folder_path) if
                           os.path.isfile(os.path.join(items_folder_path, f)) and f.endswith('.json')]

        cursor = self.__db.cursor()
        format = ",".join(["%s"]*len(artists_spot_id))
        cursor.execute(f"SELECT spotify_id, artist_id FROM Artist WHERE spotify_id in ({format})", artists_spot_id)
        artists_spot_id = cursor.fetchall()
        num_artists = len(artists_spot_id)
        start_time = time.time()

        def get_alias_names(artist_id):
            with open(os.path.join(items_folder_path, f"{artist_id}.json"), "r") as f:
                return json.load(f)['aliases']
        values = []
        for spot_id, artist_id in artists_spot_id:
            for alias_name in get_alias_names(spot_id):
                values.append((artist_id, alias_name['name']))

        cursor.executemany("INSERT IGNORE INTO Alias (artist_id, alias_name) VALUES (%s, %s)", values)
        self.__db.commit()
        end_time = time.time()

        print(f"Successfully inserted {len(values)} aliases in {end_time - start_time} seconds")

    def __insert_chapters_genres(self):
        parser = SpotifyParser('chapters', ChapterModel)
        chapters: List[ChapterModel] = parser.parse_all()
        queue = Queue()

        for chapter in chapters:
            queue.put(chapter)

        def worker():
            db = mysql.connector.connect(
                host=DATABASE_HOST,
                user=DATABASE_USER,
                password=DATABASE_PASSWORD,
                database=DATABASE_NAME
            )
            cursor = db.cursor()

            while not queue.empty():
                chapter = queue.get()
                try:
                    check_query = "SELECT EXISTS(SELECT 1 FROM Chapter WHERE spotify_id = %s)"
                    cursor.execute(check_query, (chapter.spotify_id,))
                    exists = cursor.fetchone()[0]
                    if not exists:
                        insert_query = """
                                       INSERT INTO Chapter (spotify_id, chapter_name, audio_preview_url, chapter_number, duration_ms, explicit, external_url, href, type, uri, release_date)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                       """
                        chapter_data = (
                            chapter.spotify_id, chapter.chapter_name, chapter.audio_preview_url, chapter.chapter_number,
                            chapter.description, chapter.html_description, chapter.duration_ms, chapter.explicit,
                            chapter.external_url, chapter.href, chapter.type, chapter.uri, chapter.release_date
                        )
                        cursor.execute(insert_query, chapter_data)
                        db.commit()
                        # print(f"Inserted chapter data for {chapter.spotify_id}")
                    # else:
                    # print(f"Row already exists for {chapter.spotify_id}, skipping.")
                finally:
                    queue.task_done()

        start_time = time.time()

        # Start a pool of worker threads
        threads = []
        for i in range(5):  # Number of threads
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)

        # Wait for all tasks in the queue to be processed
        queue.join()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        count_query = "SELECT COUNT(*) FROM Chapter"
        cursor = self.__db.cursor()
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]

        end_time = time.time()
        print(f"Finished inserting chapters. Total chapters in database: {row_count}")
        print(f"Finished inserting chapters in {end_time - start_time} seconds")

    def __insert_playlists(self):
        parser = SpotifyParser('playlists', PlaylistModel)
        playlists: List[PlaylistModel] = parser.parse_all()
        queue = Queue()

        for playlist in playlists:
            queue.put(playlist)

        def worker():
            db = mysql.connector.connect(
                host=DATABASE_HOST,
                user=DATABASE_USER,
                password=DATABASE_PASSWORD,
                database=DATABASE_NAME
            )
            cursor = db.cursor()

            while not queue.empty():
                playlist: PlaylistModel = queue.get()
                try:
                    check_query = "SELECT EXISTS(SELECT 1 FROM Playlist WHERE spotify_id = %s)"
                    cursor.execute(check_query, (playlist.spotify_id,))
                    exists = cursor.fetchone()[0]
                    if not exists:
                        insert_query = """
                                          INSERT INTO Playlist (spotify_id, playlist_name, description, nb_followers, collaborative, snapshot_id, href, external_url, uri)
                                          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                       """
                        playlist_data = (
                            playlist.spotify_id, playlist.playlist_name, playlist.description, playlist.nb_followers,
                            playlist.collaborative, playlist.snapshot_id, playlist.href, playlist.external_url,
                            playlist.uri
                        )
                        cursor.execute(insert_query, playlist_data)
                        db.commit()
                finally:
                    queue.task_done()

        start_time = time.time()

        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)

        queue.join()

        for thread in threads:
            thread.join()

        count_query = "SELECT COUNT(*) FROM Playlist"
        cursor = self.__db.cursor()
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]

        end_time = time.time()
        print(f"Finished inserting playlists. Total playlists in database: {row_count}")
        print(f"Finished inserting playlists in {end_time - start_time} seconds")

    def __insert_tracks(self):
        start_time = time.time()
        parser = SpotifyParser('tracks', TrackModel)
        tracks: List[TrackModel] = parser.parse_all()
        q = Queue()
        tracks_to_insert = Queue()
        batch_count = 10000
        for track in tracks:
            q.put(track)
        
        del tracks
        
        def get_items(q:Queue, count):
            items = []
            for _ in range(count):
                if (q.empty()):
                    break
                items.append(q.get())
            return items
        
        check_query = f"SELECT spotify_id from Audio where spotify_id in ({",".join((["%s"]*batch_count))})"
        print("Remove all spotify ids to remove...")
        while(not q.empty()):
            items = get_items(q, batch_count)    
            if len(items) != batch_count:
                query = f"SELECT spotify_id from Audio where spotify_id in ({",".join((["%s"]*len(items)))})"
            else:
                query = check_query
            tracks_ids = map(lambda t: t.spotify_id, items)
            cursor = self.__db.cursor()
            cursor.execute(query, list(tracks_ids))
            existing_ids = set(cursor.fetchall())
            filtered_tracks = filter(lambda track: track.spotify_id not in existing_ids, items)
            for t in filtered_tracks:
                tracks_to_insert.put(t)
        
        print("Insert new tracks to tables ...")
        insert_audio_stmt = """INSERT INTO Audio (spotify_id, audio_name, uri, href, external_url, explicit) VALUES (%s, %s, %s, %s, %s, %s)"""
        insert_track_stmt = """INSERT INTO Track (track_id, popularity, type, duration_ms, preview_url, disc_number) VALUES (%s, %s, %s, %s, %s, %s)"""
        select_id_query =f"SELECT spotify_id, audio_id FROM Audio where spotify_id in ({",".join(["%s"]*batch_count)})"
        while(not tracks_to_insert.empty()):
            items: List[TrackModel] = get_items(tracks_to_insert, batch_count)
            audio_tuples = map(lambda t: (t.spotify_id, t.audio_name, t.uri, t.href, t.external_url,t.explicit), items)
            cursor = self.__db.cursor()
            stmt_tuples = list(audio_tuples)
            cursor.executemany(insert_audio_stmt, stmt_tuples)
            ids = map(lambda t: t[0], stmt_tuples)
            if (len(items) != batch_count):
                query = f"SELECT spotify_id, audio_id FROM Audio where spotify_id in ({",".join(["%s"]*len(items))})"
            else:
                query = select_id_query
            cursor.execute(query, list(ids))
            spot_audio_id = cursor.fetchall()
            audio_spot_id = {spot_id: audio_id for spot_id, audio_id in spot_audio_id}
            records = map(lambda t: (audio_spot_id[t.spotify_id], t.popularity, t.type, t.duration_ms, t.preview_url, t.disc_number), items)
            cursor.executemany(insert_track_stmt, list(records))
        

        count_query = "SELECT COUNT(*) FROM Track"
        cursor = self.__db.cursor()
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]
        end_time = time.time()
        print(f"Finished inserting tracks. Total tracks in database: {row_count}")
        print(f"Finished inserting tracks in {end_time - start_time} seconds")

    def __insert_audiobooks(self):
        parser = SpotifyParser('audiobooks', AudiobookModel)
        audiobooks: List[AudiobookModel] = parser.parse_all()
        queue = Queue()

        for audiobook in audiobooks:
            queue.put(audiobook)

        def worker():
            db = mysql.connector.connect(
                host=DATABASE_HOST,
                user=DATABASE_USER,
                password=DATABASE_PASSWORD,
                database=DATABASE_NAME
            )
            cursor = db.cursor()

            while not queue.empty():
                audiobook: AudiobookModel = queue.get()
                try:
                    check_query = "SELECT EXISTS(SELECT 1 FROM Audio WHERE spotify_id = %s)"
                    cursor.execute(check_query, (audiobook.spotify_id,))
                    exists = cursor.fetchone()[0]
                    if not exists:
                        insert_query = """
                                          INSERT INTO Audio (spotify_id, audio_name, uri, href, external_url, explicit)
                                          VALUES (%s, %s, %s, %s, %s, %s)
                                        """
                        audiobook_data = (
                            audiobook.spotify_id, audiobook.audio_name, audiobook.uri, audiobook.href,
                            audiobook.external_url, audiobook.explicit
                        )
                        cursor.execute(insert_query, audiobook_data)
                        db.commit()

                        audio_id = cursor.lastrowid

                        insert_query = """
                                          INSERT INTO Audiobook (audiobook_id, description, edition, publisher, total_chapters, media_type)
                                          VALUES (%s, %s, %s, %s, %s, %s)
                                        """
                        audiobook_data = (
                            audio_id, audiobook.description, audiobook.edition, audiobook.publisher,
                            audiobook.total_chapters, audiobook.media_type
                        )
                        cursor.execute(insert_query, audiobook_data)
                        db.commit()
                    #     print(f"Inserted audiobook data for {audiobook.spotify_id}")
                    # else:
                    #     print(f"Row already exists for {audiobook.spotify_id}, skipping.")
                finally:
                    queue.task_done()

        start_time = time.time()

        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker)
            thread.start()
            threads.append(thread)

        queue.join()

        for thread in threads:
            thread.join()

        count_query = "SELECT COUNT(*) FROM Audiobook"
        cursor = self.__db.cursor()
        cursor.execute(count_query)
        row_count = cursor.fetchone()[0]

        end_time = time.time()
        print(f"Finished inserting audiobooks. Total audiobooks in database: {row_count}")
        print(f"Finished inserting audiobooks in {end_time - start_time} seconds")

    def __insert_tracks_artists(self):
        parser = SpotifyParser('tracks', TrackModel)
        tracks: List[TrackModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'artists', json_data.get('artists')) or mapped_object
            )
        )

        # Use buffered cursor to avoid "Unread result found" error
        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        for track in tracks:
            cursor.execute("SELECT audio_id FROM Audio WHERE spotify_id = %s", (track.spotify_id,))
            result = cursor.fetchone()
            if result:
                track_id = result[0]
                # Verify the track_id in Track table
                cursor.execute("SELECT EXISTS(SELECT 1 FROM Track WHERE track_id = %s)", (track_id,))
                exists = cursor.fetchone()[0]
                if not exists:
                    continue
            else:
                continue

            for artist in track.artists:
                cursor.execute("SELECT artist_id FROM Artist WHERE artist_name = %s", (artist['name'],))
                result = cursor.fetchone()
                if result:
                    artist_id = result[0]
                else:
                    continue

                cursor.execute("SELECT COUNT(1) FROM Tracks_Artists WHERE track_id = %s AND artist_id = %s",
                               (track_id, artist_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO Tracks_Artists (track_id, artist_id) VALUES (%s, %s)",
                                   (track_id, artist_id))
                    self.__db.commit()
                    num_inserts += 1

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} track-artist relationships in {end_time - start_time} seconds")

    def __insert_tracks_albums(self):
        parser = SpotifyParser('tracks', TrackModel)
        tracks: List[TrackModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'album', json_data.get('album')) or mapped_object
            )
        )

        # Use buffered cursor to avoid "Unread result found" error
        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        for track in tracks:
            cursor.execute("SELECT audio_id FROM Audio WHERE spotify_id = %s", (track.spotify_id,))
            result = cursor.fetchone()
            if result:
                track_id = result[0]
                # Verify the track_id in Track table
                cursor.execute("SELECT EXISTS(SELECT 1 FROM Track WHERE track_id = %s)", (track_id,))
                exists = cursor.fetchone()[0]
                if not exists:
                    continue
            else:
                continue

            album = track.album

            cursor.execute("SELECT album_id FROM Album WHERE album_name = %s", (album['name'],))
            result = cursor.fetchone()
            if result:
                album_id = result[0]
            else:
                continue

            cursor.execute("SELECT COUNT(1) FROM Tracks_Albums WHERE track_id = %s AND album_id = %s",
                           (track_id, album_id))
            if cursor.fetchone()[0] == 0:
                cursor.execute("INSERT INTO Tracks_Albums (track_id, album_id) VALUES (%s, %s)",
                               (track_id, album_id))
                self.__db.commit()
                num_inserts += 1
            #     print(f"Inserted track album data for {track.spotify_id}, {album['name']}")
            # else:
            #     print(f"Row already exists for {track.spotify_id}, {album['name']}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} track-album relationships in {end_time - start_time} seconds")

    def __insert_available_markets_albums(self):
        parser = SpotifyParser('albums', AlbumModel)
        albums: List[AlbumModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'available_markets', json_data.get('available_markets')) or mapped_object
            )
        )

        # Use buffered cursor to avoid "Unread result found" error
        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        # Get all markets and their ids
        cursor.execute("SELECT market_id, country_code FROM Market")
        markets = cursor.fetchall()
        market_map = {}
        for market in markets:
            market_id, country_code = market
            market_map[country_code] = market_id

        for album in albums:
            cursor.execute("SELECT album_id FROM Album WHERE spotify_id = %s", (album.spotify_id,))
            album_id = cursor.fetchone()

            if not album_id:
                continue

            album_id = album_id[0]

            for market in album.available_markets:
                market_id = market_map[market]

                cursor.execute("SELECT COUNT(1) FROM Available_Markets_Albums WHERE market_id = %s AND album_id = %s",
                               (market_id, album_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO Available_Markets_Albums (album_id, market_id) VALUES (%s, %s)",
                                   (album_id, market_id))
                    self.__db.commit()
                    num_inserts += 1
                #     print(f"Inserted album, track data for {album_id}, {market_id}")
                # else:
                #     print(f"Row already exists for {album_id}, {market_id}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} market-album relationships in {end_time - start_time} seconds")

    def __insert_available_markets_tracks(self):
        parser = SpotifyParser('tracks', TrackModel)
        tracks: List[TrackModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'available_markets', json_data.get('available_markets')) or mapped_object
            )
        )

        # Use buffered cursor to avoid "Unread result found" error
        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        # Get all markets and their ids
        cursor.execute("SELECT market_id, country_code FROM Market")
        markets = cursor.fetchall()
        market_map = {}
        for market in markets:
            market_id, country_code = market
            market_map[country_code] = market_id

        for track in tracks:
            # Check if track exists in Audio table
            cursor.execute("SELECT audio_id FROM Audio WHERE spotify_id = %s", (track.spotify_id,))
            result = cursor.fetchone()
            if not result:
                continue

            track_id = result[0]

            # Check if track exists in Track table
            cursor.execute("SELECT EXISTS(SELECT 1 FROM Track WHERE track_id = %s)", (track_id,))
            exists = cursor.fetchone()[0]
            if not exists:
                continue

            for market in track.available_markets:
                market_id = market_map[market]

                cursor.execute("SELECT COUNT(1) FROM Available_Markets_Tracks WHERE market_id = %s AND track_id = %s",
                               (market_id, track_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO Available_Markets_Tracks (track_id, market_id) VALUES (%s, %s)",
                                   (track_id, market_id))
                    self.__db.commit()
                    num_inserts += 1
                #     print(f"Inserted market, track data for {market_id}, {track_id}")
                # else:
                #     print(f"Row already exists for {market_id}, {track_id}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} market-track relationships in {end_time - start_time} seconds")

    def __insert_playlists_tracks(self):
        parser = SpotifyParser('playlists', PlaylistModel)
        playlists: List[PlaylistModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'tracks', json_data.get('tracks')) or mapped_object
            )
        )

        # Use buffered cursor to avoid "Unread result found" error
        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        for playlist in playlists:
            cursor.execute("SELECT playlist_id FROM Playlist WHERE spotify_id = %s", (playlist.spotify_id,))
            playlist_id = cursor.fetchone()

            if not playlist_id:
                continue

            playlist_id = playlist_id[0]
            for track in playlist.tracks['items']:
                track = track['track']
                if track is None or track['id'] is None:
                    continue
                cursor.execute("SELECT audio_id FROM Audio WHERE spotify_id = %s", (track['id'],))
                result = cursor.fetchone()
                if not result:
                    continue

                track_id = result[0]

                # Check if track exists in Track table
                cursor.execute("SELECT EXISTS(SELECT 1 FROM Track WHERE track_id = %s)", (track_id,))
                exists = cursor.fetchone()[0]
                if not exists:
                    continue

                cursor.execute("SELECT COUNT(1) FROM Playlists_Tracks WHERE playlist_id = %s AND track_id = %s",
                               (playlist_id, track_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO Playlists_Tracks (track_id, playlist_id) VALUES (%s, %s)",
                                   (track_id, playlist_id))
                    self.__db.commit()
                    num_inserts += 1
                #     print(f"Inserted playlist, track for {playlist_id}, {track_id}")
                # else:
                #     print(f"Row already exists for {playlist_id}, {track_id}, skipping.")

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} market-track relationships in {end_time - start_time} seconds")

    def __insert_audiobooks_authors(self):
        parser = SpotifyParser('authors', AuthorModel)
        audiobooks: List[AuthorModel] = parser.parse_all()

        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        for authors in audiobooks:
            spotify_id = authors.audiobook_id

            cursor.execute("SELECT audio_id FROM Audio WHERE spotify_id = %s", (spotify_id,))
            result = cursor.fetchone()
            if result:
                audiobook_id = result[0]
                # Verify the audiobook_id in Audiobook table
                cursor.execute("SELECT EXISTS(SELECT 1 FROM Audiobook WHERE audiobook_id = %s)", (audiobook_id,))
                exists = cursor.fetchone()[0]
                if not exists:
                    continue
            else:
                continue

            for author in authors.authors:
                cursor.execute("SELECT author_id FROM Author WHERE author_name = %s", (author['name'],))
                result = cursor.fetchone()

                if not result:
                    continue

                author_id = result[0]

                cursor.execute("SELECT COUNT(1) FROM Audiobooks_Authors WHERE audiobook_id = %s AND author_id = %s",
                               (audiobook_id, author_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO Audiobooks_Authors (audiobook_id, author_id) VALUES (%s, %s)",
                                   (audiobook_id, author_id))
                    self.__db.commit()
                    num_inserts += 1
                #     print('Inserted')
                # else:
                #     print('Already inserted')

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} audiobook-author relationships in {end_time - start_time} seconds")

    def __insert_authors(self):
        parser = SpotifyParser('authors', AuthorModel)
        audiobooks: List[AuthorModel] = parser.parse_all()

        cursor = self.__db.cursor(buffered=True)

        for authors in audiobooks:
            for author in authors.authors:
                cursor.execute("SELECT author_id FROM Author WHERE author_name = %s", (author['name'],))
                result = cursor.fetchone()

                if not result:
                    cursor.execute("INSERT INTO Author (author_name) VALUES (%s)", (author['name'],))
                    self.__db.commit()
                #     print(f"Inserted author: {author['name']}")
                # else:
                #     print(f"Author already exists: {author['name']}")

    def __insert_audiobooks_chapters(self):
        parser = SpotifyParser('audiobooks', AudiobookModel)
        audiobooks: List[AudiobookModel] = parser.parse_all(
            middleware=lambda mapped_object, json_data: (
                setattr(mapped_object, 'chapters', json_data.get('chapters')) or mapped_object
            )
        )

        cursor = self.__db.cursor(buffered=True)
        start_time = time.time()
        num_inserts = 0

        for audiobook in audiobooks:
            spotify_id = audiobook.spotify_id

            cursor.execute("SELECT audio_id FROM Audio WHERE spotify_id = %s", (spotify_id,))
            result = cursor.fetchone()
            if result:
                audiobook_id = result[0]
                # Verify the audiobook_id in Audiobook table
                cursor.execute("SELECT EXISTS(SELECT 1 FROM Audiobook WHERE audiobook_id = %s)", (audiobook_id,))
                exists = cursor.fetchone()[0]
                if not exists:
                    continue
            else:
                continue

            items = audiobook.chapters['items']
            for chapter in items:
                cursor.execute("SELECT chapter_id FROM Chapter WHERE spotify_id = %s", (chapter['id'],))
                result = cursor.fetchone()

                if not result:
                    continue

                chapter_id = result[0]

                cursor.execute("SELECT COUNT(1) FROM Audiobooks_Chapters WHERE audiobook_id = %s AND chapter_id = %s",
                               (audiobook_id, chapter_id))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("INSERT INTO Audiobooks_Chapters (audiobook_id, chapter_id) VALUES (%s, %s)",
                                   (audiobook_id, chapter_id))
                    self.__db.commit()
                    num_inserts += 1
                #     print('Inserted')
                # else:
                #     print('Already inserted')

        end_time = time.time()
        print(f"Successfully inserted {num_inserts} audiobook-author relationships in {end_time - start_time} seconds")
