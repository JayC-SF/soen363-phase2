CREATE VIEW ArtistsWithGenres AS
SELECT Artist.artist_id,
       Artist.artist_name,
       Genre.genre_name
FROM Artist
         INNER JOIN Artists_Genres ON Artist.artist_id = Artists_Genres.artist_id
         INNER JOIN Genre ON Artists_Genres.genre_id = Genre.genre_id;