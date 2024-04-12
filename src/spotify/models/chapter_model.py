class ChapterModel:
    def __init__(self, spotify_id, chapter_name, audio_preview_url, chapter_number, duration_ms, explicit, external_url,
                 href, type, uri, release_date):
        self.spotify_id = spotify_id
        self.chapter_name = chapter_name
        self.audio_preview_url = audio_preview_url
        self.chapter_number = chapter_number
        self.duration_ms = duration_ms
        self.explicit = explicit
        self.external_url = external_url
        self.href = href
        self.type = type
        self.uri = uri
        self.release_date = release_date
