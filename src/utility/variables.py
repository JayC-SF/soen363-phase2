from dotenv import load_dotenv
from os import getenv
from os.path import abspath, join, dirname

load_dotenv(abspath(join(dirname(__file__), "../.env")))

# CONSTANTS
SLEEP_TIMER_AVG = 10

# LOAD ENVIRONMENT VARIABLES
SPOTIFY_CLIENT_ID = getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = getenv("SPOTIFY_CLIENT_SECRET")
DATABASE_HOST = getenv("DATABASE_HOST")
DATABASE_NAME = getenv("DATABASE_NAME")
DATABASE_USER = getenv("DATABASE_USER")
DATABASE_PASSWORD = getenv("DATABASE_PASSWORD")

# DEFINE PROGRAM CONSTANTS
DATA_PATH = abspath(join(dirname(__file__), "../../data"))
TEMP_PATH = abspath(join(dirname(__file__), "../../tmp"))

# DEFINE SPOTIFY CONSTANTS
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_RATE_LIMIT_RESPONSE_CODE = 429
SPOTIFY_BATCH_MAX_ITEMS = 20
SPOTIFY_ITEMS_CSV_NAME = "ids.csv"
SPOTIFY_ITEMS_FOLDER_NAME = "items"
SPOTIFY_MAPPING_FILE_NAME = "mapping.json"
SPOTIFY_API_URL = "https://api.spotify.com/v1"
SPOTIFY_DATA_PATH = abspath(join(DATA_PATH, 'spotify'))
DB_CSV_PATH = join(DATA_PATH, 'db/csv')

# DEFINE MUSICBRAINZ CONSTANTS
MUSICBRAINZ_DATA_PATH = abspath(join(DATA_PATH, 'musicbrainz'))
MUSICBRAINZ_ITEMS_CSV_NAME = "artist_names.csv"
MUSICBRAINZ_ITEMS_FOLDER_NAME = "items"
MUSICBRAINZ_API_URL = "https://musicbrainz.org/ws/2/release"
MUSICBRAINZ_ARTISTS_DATA_ITEMS = abspath(join(SPOTIFY_DATA_PATH, 'artists/items'))

# CONSTANT FOR DATA CHOICES TO BE INSERTED INTO THE DB
DATA_CHOICES = ['playlists', 'tracks', 'artists', 'albums', 'audiobooks', 'genres', 'artists_genres', 'chapters',
                'markets', 'authors', 'aliases', 'tracks_artists', 'artists_aliases', 'tracks_albums', 'artists_genres',
                'available_markets_albums', 'available_markets_tracks', 'playlists_tracks', 'audiobooks_authors',
                'audiobooks_chapters']
