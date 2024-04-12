from spotify.models.audio_model import AudioModel


class AudiobookModel(AudioModel):
    def __init__(self, spotify_id, audio_name, uri, href, external_url, explicit, description, edition, publisher,
                 total_chapters, media_type, chapters=None):
        AudioModel.__init__(self, spotify_id, audio_name, uri, href, external_url, explicit)
        self.description = description
        self.edition = edition
        self.publisher = publisher
        self.total_chapters = total_chapters
        self.media_type = media_type
        self.chapters = chapters
