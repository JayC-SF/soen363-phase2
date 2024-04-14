match (n) detach delete n;
drop index Audio_audio_id if exists;
drop index Track_track_id if exists;
drop index Audiobook_audiobook_id if exists;
drop index Artist_artist_id if exists;
drop index Playlist_playlist_id if exists;
drop index Album_album_id if exists;
drop index Genre_genre_id if exists;
drop index Market_market_id if exists;
drop index Author_author_id if exists;
drop index Chapter_chapter_id if exists;


load csv with headers from 'file:///Audio.csv' as row
create (a: Audio {
    audio_id: toInteger(row.audio_id),
    audio_name:row.audio_name,
    spotify_id:row.spotify_id,
    uri:row.uri,
    href:row.href,
    external_url:row.external_url,
    explicit: toBoolean(row.explicit)
});

create index Audio_audio_id if not exists for (a:Audio) on (a.audio_id);

// load tracks
load csv with headers from 'file:///Track.csv' as row
create (t: Track {
    track_id: toInteger(row.track_id),
    popularity: toInteger(row.popularity),
    type: toInteger(row.type),
    duration_ms: row.duration_ms,
    preview_url: row.preview_url,
    disc_number: toInteger(row.disc_number)
});

// create indexes for speed up
create index Track_track_id if not exists for (t:Track) on (t.track_id);

// remove isa relationship from tracks
match (t:Track)
match (a: Audio {audio_id:t.track_id})
set t.track_name = a.audio_name,
    t.spotify_id = a.spotify_id,
    t.uri = a.uri,
    t.href = a.href,
    t.external_url = a.external_url,
    t.explicit = a.explicit
detach delete a;

load csv with headers from 'file:///Audiobook.csv' as row
create (b: Audiobook {
    audiobook_id: toInteger(row.audiobook_id),
    description: row.description,
    edition: row.edition,
    publisher: row.publisher,
    total_chapters: toInteger(row.total_chapters),
    media_type: row.media_type
});

create index Audiobook_audiobook_id if not exists for (a:Audiobook) on (a.audiobook_id);

// load tracks remove audio isa relationship
match (b:Audiobook)
match (a: Audio {audio_id:b.audiobook_id})
set b.audiobook_name = a.audio_name,
    b.spotify_id = a.spotify_id,
    b.uri = a.uri,
    b.href = a.href,
    b.external_url = a.external_url,
    b.explicit = a.explicit
detach delete a;

// Remove audio entity
match (a:Audio) detach delete a;
drop index Audio_audio_id if exists;

load csv with headers from 'file:///Artist.csv' as row
create (a:Artist {
    artist_id: toInteger(row.artist_id),
    spotify_id: row.spotify_id,
    artist_name: row.artist_name,
    nb_followers: toInteger(row.nb_followers),
    popularity: toInteger(row.popularity),
    external_url: row.external_url,
    href: row.href,
    uri: row.uri,
    aliases: []
});

// create artist index for faster lookups
create index Artist_artist_id if not exists for (a:Artist) on (a.artist_id);

load csv with headers from 'file:///Playlist.csv' as row
create (p:Playlist {
    playlist_id: toInteger(row.playlist_id),
    spotify_id: row.spotify_id,
    playlist_name: row.playlist_name,
    description: row.description,
    nb_followers: toInteger(row.nb_followers),
    collaborative: row.collaborative,
    snapshot_id: row.snapshot_id,
    uri: row.uri,
    href: row.href,
    external_url: row.external_url
});

create index Playlist_playlist_id if not exists for (a:Playlist) on (a.playlist_id);


// Load aliases as nodes for better performance
load csv with headers from 'file:///Alias.csv' as row
create (a:Alias {
    artist_id:toInteger(row.artist_id), 
    alias_name: row.alias_name
});


// add alias to artist.aliases and delete
match (al:Alias)
match (a: Artist {artist_id: al.artist_id})
set a.aliases = a.aliases + al.alias_name
delete al;

load csv with headers from 'file:///Album.csv' as row
create (a:Album {
    album_id: toInteger(row.album_id),
    album_name: row.album_name,
    spotify_id: row.spotify_id,
    total_tracks: toInteger(row.total_tracks),
    popularity: toInteger(row.popularity),
    release_date: date(row.release_date),
    label: row.label,
    external_url: row.external_url,
    href: row.href,
    type: row.type
});

// create index for album_id
create index Album_album_id if not exists for (a:Album) on (a.album_id);

load csv with headers from 'file:///Genre.csv' as row
create (g:Genre {
    genre_id: toInteger(row.genre_id),
    genre_name: row.genre_name
});

// create index for genre_id
create index Genre_genre_id if not exists for (g:Genre) on (g.genre_id);


load csv with headers from 'file:///Market.csv' as row
create (m: Market {
    market_id: toInteger(row.market_id),
    country_code: row.country_code
});

create index Market_market_id if not exists for (m:Market) on (m.market_id);

load csv with headers from 'file:///Author.csv' as row
create (a: Author {
    author_id: toInteger(row.author_id),
    author_name: row.author_name
});

create index Author_author_id if not exists for (a:Author) on (a.author_id);

load csv with headers from 'file:///Chapter.csv' as row
create (c: Chapter {
    chapter_id: toInteger(row.chapter_id),
    spotify_id: row.spotify_id,
    chapter_name: row.chapter_name,
    audio_preview_url: row.audio_preview_url,
    chapter_number: toInteger(row.chapter_number),
    duration_ms: toInteger(row.duration_ms),
    explicit: toBoolean(row.explicit),
    external_url: row.external_url,
    href: row.href,
    type: row.type,
    uri: row.uri,
    release_date: row.release_date
});

create index Chapter_chapter_id if not exists for (c:Chapter) on (c.chapter_id);

load csv with headers from 'file:///Tracks_Artists.csv' as row
match (t: Track {track_id:toInteger(row.track_id)})
match (a: Artist{artist_id:toInteger(row.artist_id)})
create (a)-[:ProducedTrack]->(t);


load csv with headers from 'file:///Tracks_Albums.csv' as row
match (t: Track {track_id:toInteger(row.track_id)})
match (a: Album{album_id:toInteger(row.album_id)})
create (a)-[:ContainsTrackInAlbum]->(t);

load csv with headers from 'file:///Artists_Genres.csv' as row
match (a: Artist{artist_id:toInteger(row.artist_id)})
match (g: Genre{genre_id: toInteger(row.genre_id)})
create (a)-[:OfGenre]->(g);

load csv with headers from 'file:///Available_Markets_Albums.csv' as row
match (a: Album {album_id: toInteger(row.album_id)})
match (m: Market {market_id: toInteger(row.market_id)})
create (a)-[:AvailableIn]->(m);

load csv with headers from 'file:///Available_Markets_Tracks.csv' as row
match (t: Track {track_id: toInteger(row.track_id)})
match (m: Market {market_id: toInteger(row.market_id)})
create (t)-[:AvailableIn]->(m);


load csv with headers from 'file:///Playlists_Tracks.csv' as row
match (t: Track {track_id: toInteger(row.track_id)})
match (p: Playlist {playlist_id: toInteger(row.playlist_id)})
create (p)-[:ContainsTrackInPlaylist]->(t);

load csv with headers from 'file:///Audiobooks_Authors.csv' as row
match (ad: Audiobook {audiobook_id: toInteger(row.audiobook_id)})
match (at: Author {author_id: toInteger(row.author_id)})
create (at)-[:ProducedAudiobook]->(ad);