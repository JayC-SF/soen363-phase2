class PlaylistModel:
    def __init__(self, spotify_id, playlist_name, description, nb_followers, collaborative, snapshot_id, href,
                 external_url, uri, tracks=None):
        self.spotify_id = spotify_id
        self.playlist_name = playlist_name
        self.description = description
        self.nb_followers = nb_followers
        self.collaborative = collaborative
        self.snapshot_id = snapshot_id
        self.uri = uri
        self.href = href
        self.external_url = external_url
        self.uri = uri
        self.tracks = tracks
