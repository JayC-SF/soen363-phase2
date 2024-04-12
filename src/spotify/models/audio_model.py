class AudioModel:
    def __init__(self, spotify_id, audio_name, uri, href, external_url, explicit):
        self.audio_name = audio_name
        self.spotify_id = spotify_id
        self.uri = uri
        self.href = href
        self.external_url = external_url
        self.explicit = explicit
