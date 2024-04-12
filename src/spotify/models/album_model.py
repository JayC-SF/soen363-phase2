class AlbumModel:
    def __init__(self, spotify_id, album_name, total_tracks, popularity, release_date, label, external_url, href, type,
                 available_markets=None):
        self.spotify_id = spotify_id
        self.album_name = album_name
        self.total_tracks = total_tracks
        self.popularity = popularity
        self.release_date = release_date
        self.label = label
        self.external_url = external_url
        self.href = href
        self.type = type
        self.available_markets = available_markets
