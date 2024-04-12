from utility import variables as var
from database.inserter import DatabaseInserter
from musicbrainz.loader import AliasLoader
from spotify import loader, scraper
from argparse import ArgumentParser

from utility.auth_token import SPOTIFY_AUTH_TOKEN
from utility.variables import DATA_CHOICES


def main(args):
    # SPOTIFY_AUTH_TOKEN.get_authorization()
    if args.scrape_playlists:
        # spotify doesn't scrape playlists with batchmode
        playlists_scraper = scraper.SpotifyScraper("playlists")
        playlists_scraper.scrape_items()
        pass
    if args.load_info_from_playlists:
        loader.load_info_from_playlists()
    if args.scrape_tracks:
        # 45 tracks per batch
        tracks_scraper = scraper.SpotifyScraper("tracks")
        tracks_scraper.scrape_items(batchmode=50)
    if args.scrape_artists:
        # 45 tracks per batch
        artists_scraper = scraper.SpotifyScraper("artists")
        artists_scraper.scrape_items(batchmode=45)
    if args.scrape_albums:
        # 20 tracks per batch
        albums_scraper = scraper.SpotifyScraper("albums")
        albums_scraper.scrape_items(batchmode=20)
    if args.scrape_audiobooks:
        audiobooks_scraper = scraper.SpotifyScraper("audiobooks")
        audiobooks_scraper.scrape_items(batchmode=45)
    if args.load_genres:
        loader.load_genres()
    if args.generate_artist_ids:
        artists_generate = scraper.SpotifyScraper("artists")
        artists_generate.generate_artists_ids()
    if args.generate_playlist_ids:
        playlists_generate = scraper.SpotifyScraper("playlists")
        playlists_generate.generate_playlist_ids()
    if args.load_musicbrainz_ids:
        loader_musicbrainz = AliasLoader("aliases")
        loader_musicbrainz.write_artist_data_to_csv()
    if args.generate_musicbrainz_aliases_json:
        generate_musicbrainz = AliasLoader("aliases")
        generate_musicbrainz.load_aliases_items()
    if args.load_authors_from_audiobooks:
        loader.load_authors_from_audiobooks()
    if args.load_chapters_from_audiobooks:
        loader.load_chapters_from_audiobooks()


def insert_to_database(args):
    if args.list:
        print(DATA_CHOICES)
    if args.data_type:
        inserter = DatabaseInserter(data_type=args.data_type)
        inserter.insert_data()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('-p', '--scrape-playlists', help=f'Scrapes playlists defined in csv file', action='store_true')
    parser.add_argument('-t', '--scrape-tracks', help=f'Scrapes tracks defined in csv file', action='store_true')
    parser.add_argument('-a', '--scrape-artists', help=f'Scrapes artists defined in csv file', action='store_true')
    parser.add_argument('-u', '--scrape-albums', help=f'Scrapes albums defined in csv file', action='store_true')
    parser.add_argument('-b', '--scrape-audiobooks', help=f'Scrapes audiobooks defined in csv file', action='store_true')
    parser.add_argument('-l', '--load-info-from-playlists', help=f'Loads information from playlists into other entities\'s csv', action='store_true')
    parser.add_argument('-g', '--load-genres', help=f'Loads all genres from albums and playlists into a csv file', action='store_true')
    parser.add_argument('-k', '--load-artists-from-tracks', help=f'Loads artists from tracks', action='store_true')
    parser.add_argument('-al', '--generate-artist-ids', help=f'Populates `ids.csv` of artists/ with list of ids', action='store_true')
    parser.add_argument('-pl', '--generate-playlist-ids', help=f'Populates `ids.csv` of playlists/ with list of playlists based on the Spotify Featured Playlists', action='store_true')
    parser.add_argument('-m', '--load-musicbrainz-ids', help=f'Populates `artist_names.csv` of "alias_ids.csv" inside the musicbrainz folder', action='store_true')
    parser.add_argument('-ml', '--generate-musicbrainz-aliases-json', help=f'Fetch MusicBrainz API for every artist name', action='store_true')
    parser.add_argument('-o', '--load-authors-from-audiobooks', help=f'Loads authors from audiobooks', action='store_true')
    parser.add_argument('-c', '--load-chapters-from-audiobooks', help=f'Loads chapters from audiobooks', action='store_true')
    subparsers = parser.add_subparsers(dest='command', help='Sub-command help')
    insert_parser = subparsers.add_parser('insert-to-database', help='Insert data to the database')
    insert_parser.add_argument('-l', '--list', help='Show the list of possible data choices', action='store_true')
    insert_parser.add_argument('-d', '--data-type', choices=DATA_CHOICES, help='Specify the type of data to insert')
    insert_parser.set_defaults(func=insert_to_database)

    # Run below if token expires at root dir through src/main_spotify_scraper.py
    # SPOTIFY_AUTH_TOKEN.refresh_token()
    # print(SPOTIFY_AUTH_TOKEN.get_authorization())

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        main(args)
