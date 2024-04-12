class ArtistModel:
    def __init__(self, spotify_id, artist_name, nb_followers, popularity, external_url, href, uri, genres=None):
        self.spotify_id = spotify_id
        self.artist_name = artist_name
        self.nb_followers = nb_followers
        self.popularity = popularity
        self.external_url = external_url
        self.href = href
        self.uri = uri
        self.genres = genres
