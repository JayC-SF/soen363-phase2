from spotify.models.audio_model import AudioModel


class TrackModel(AudioModel):
    def __init__(self, spotify_id, audio_name, uri, href, external_url, explicit, popularity, type, duration_ms,
                 preview_url, disc_number, artists=None, album=None, available_markets=None):
        AudioModel.__init__(self, spotify_id, audio_name, uri, href, external_url, explicit)
        self.popularity = popularity
        self.type = type
        self.duration_ms = duration_ms
        self.preview_url = preview_url
        self.disc_number = disc_number
        self.artists = artists
        self.album = album
        self.available_markets = available_markets
