-- Active: 1711771610747@@walidoow.com@3306@Test

-- Query Implementation
-- You need to provide demonstrate the following query types:

-- 1. Basic select with simple where clause.
SELECT * FROM Track
WHERE duration_ms > 300000;

-- 2. Basic select with simple group by clause (with and without having clause).

-- Retrieve the total number of tracks for each album
SELECT album_id, album_name, release_date, COUNT(track_id) AS total_tracks
FROM Album 
INNER JOIN Tracks_Albums USING(album_id)
GROUP BY album_id;

-- Retrieve albums that have more than 10 tracks
SELECT album_id, album_name, release_date, COUNT(track_id) AS total_tracks
FROM Album 
INNER JOIN Tracks_Albums USING(album_id)
GROUP BY album_id
HAVING COUNT(track_id) > 10;

-- 3. A simple join select query using cartesian product and where clause vs. a join query using on.

-- Cartesian product with WHERE clause. 
-- Listing Artists and all of their aliases for all artists. 
-- If an artist does not have an alias, it will not be shown as a record in the result
SELECT Artist.artist_id, Artist.artist_name, Alias.alias_name FROM Artist, Alias
WHERE Artist.artist_id = Alias.artist_id;

-- Join query using ON keyword.
-- Same query as the one right above.
SELECT artist_id, artist_name, alias_name FROM Artist
INNER JOIN Alias ON Artist.artist_id = Alias.artist_id;

-- 4. A few queries to demonstrate various join types on the same tables: inner vs. outer (left and right) vs. full join. Use of null values in the database to show the differences is required.

-- Select all columns joining Albums and Tracks where album_id is NULL.
-- This query shows all Albums that do not have tracks using LEFT JOIN.
SELECT album_id, album_name, track_id FROM Album
LEFT JOIN Tracks_Albums USING(album_id)
LEFT JOIN Track USING(track_id)
WHERE track_id IS NULL;

-- Select all track_ids that do not have an album using a RIGHT JOIN.
SELECT track_id, album_id FROM Album
RIGHT JOIN Tracks_Albums USING(album_id)
RIGHT JOIN Track USING(track_id)
WHERE album_id is NULL;

-- Combines the left join and right join results from the relationship of albums and tracks.
-- Combine queries above to get the full outer join between Track and Album
SELECT track_id, album_id FROM Album
LEFT JOIN Tracks_Albums USING(album_id)
LEFT JOIN Track USING(track_id)
UNION
SELECT track_id, album_id FROM Album
RIGHT JOIN Tracks_Albums USING(album_id)
RIGHT JOIN Track USING(track_id);

-- Shows Track and Album records relationship. If no relationship then the record is not shown.
SELECT track_id, album_id FROM Album
INNER JOIN Tracks_Albums USING(album_id)
INNER JOIN Track USING(track_id);

-- 5. A couple of examples to demonstrate correlated queries.

-- Correlated Subquery to Find Artists with More Than a Certain Number of Tracks:
SELECT artist_name FROM Artist a
WHERE (
    SELECT COUNT(*) FROM Tracks_Artists ta
    JOIN Track t ON ta.track_id = t.track_id
    WHERE ta.artist_id = a.artist_id
) > 5;

-- Finding Artists with More Than 10 Tracks
SELECT playlist_id, playlist_name
FROM Playlist p
WHERE (SELECT COUNT(*)
       FROM Playlists_Tracks pt
       WHERE pt.playlist_id = p.playlist_id) > 10;

-- 6. One example per set operations: intersect, union, and difference vs. their equivalences without using set operations.

-- Using Set Operation (Intersect)
-- Get all artists that did release an album named `UTOPIA`
SELECT artist_name FROM Artist
INTERSECT
SELECT artist_name FROM Tracks_Artists ta
INNER JOIN Artist a ON ta.artist_id = a.artist_id
INNER JOIN Track t ON ta.track_id = t.track_id
INNER JOIN Tracks_Albums tra ON t.track_id = tra.track_id
INNER JOIN Album al ON tra.album_id = al.album_id
WHERE al.album_name = 'UTOPIA';

-- Equivalent without Set Operation
SELECT DISTINCT a.artist_name
FROM Artist a
INNER JOIN Tracks_Artists ta ON a.artist_id = ta.artist_id
INNER JOIN Track t ON ta.track_id = t.track_id
INNER JOIN Tracks_Albums tra ON t.track_id = tra.track_id
INNER JOIN Album al ON tra.album_id = al.album_id
WHERE al.album_name = 'GUTS'
AND EXISTS (
    SELECT 1 FROM Artist a2
    INNER JOIN Tracks_Artists ta2 ON a2.artist_id = ta2.artist_id
    INNER JOIN Track t2 ON ta2.track_id = t2.track_id
    INNER JOIN Tracks_Albums tra2 ON t2.track_id = tra2.track_id
    INNER JOIN Album al2 ON tra2.album_id = al2.album_id
    WHERE al2.album_name = 'SOUR'
    AND a.artist_id = a2.artist_id
);

-- Using Set Operation (Union)
SELECT artist_name FROM Artist
WHERE nb_followers > 1000
UNION 
SELECT artist_name FROM Artist
WHERE popularity > 70;

-- Equivalent without Set Operation
SELECT artist_name FROM Artist
WHERE nb_followers > 1000 OR popularity > 70;

-- Using Set Operation (Difference)
SELECT artist_name FROM Artist
EXCEPT
SELECT artist_name FROM Tracks_Artists ta
INNER JOIN Artist a ON ta.artist_id = a.artist_id
INNER JOIN Track t ON ta.track_id = t.track_id
INNER JOIN Tracks_Albums tra ON t.track_id = tra.track_id
INNER JOIN Album al ON tra.album_id = al.album_id
WHERE al.album_name = 'HEROES & VILLAINS';

-- Equivalent without Set Operation
SELECT DISTINCT a.artist_name FROM Artist a
LEFT JOIN Tracks_Artists ta ON a.artist_id = ta.artist_id
LEFT JOIN Track t ON ta.track_id = t.track_id
LEFT JOIN Tracks_Albums tra ON t.track_id = tra.track_id
LEFT JOIN Album al ON tra.album_id = al.album_id
WHERE al.album_name != 'HEROES & VILLAINS'
OR al.album_name IS NULL;


-- 7. An example of a view that has a hard-coded criteria, by which the content of the view may change upon changing the hard-coded value (see L09 slide 24).
-- Create a view to display detailed information about popular tracks by genre
-- Popular Tracks by genre, querying all tracks with popularity of more than 80. 80 is the hardcoded value.
CREATE OR REPLACE VIEW PopularTracksByGenre AS
SELECT
    t.track_id,
    t.popularity,
    t.type AS track_type,
    t.duration_ms,
    t.preview_url,
    t.disc_number,
    a.artist_id,
    a.artist_name,
    al.album_id,
    al.album_name,
    g.genre_id,
    g.genre_name
FROM Track t
INNER JOIN Tracks_Artists ta ON t.track_id = ta.track_id
INNER JOIN Artist a ON ta.artist_id = a.artist_id
INNER JOIN Tracks_Albums tal ON t.track_id = tal.track_id
INNER JOIN Album al ON tal.album_id = al.album_id
INNER JOIN Artists_Genres ag ON a.artist_id = ag.artist_id
INNER JOIN Genre g ON ag.genre_id = g.genre_id
WHERE t.popularity > 80
ORDER BY t.popularity DESC;

-- 8. Two implementations of the division operator using a) a regular nested query using NOT IN and b) a correlated nested query using NOT EXISTS and EXCEPT.

-- a) Select all the distinct playlist_ids from Playlists_Tracks that are associated to all Track records.
--    Query is using a nested query and NOT IN operator. 
SELECT * FROM Playlists_Tracks 
WHERE playlist_id NOT IN ( 
    SELECT playlist_id FROM 
        (
            (
                SELECT x , y FROM (select y from Track ) as T 
                CROSS JOIN 
                (select distinct x from Playlists_Tracks) as PT
            )
            EXCEPT
            (SELECT x , y FROM R) 
        ) AS NOT_ASSOCIATED_WITH_ALL 
); 

-- b) Same as a) but done with correlated nested query using NOT EXISTS and EXCEPT.
-- NOTE: the query takes a couple of minutes to complete...

SELECT * FROM Playlists_Tracks as PT1
WHERE NOT EXISTS (
    (SELECT T.track_id FROM Track as T)
    EXCEPT
    (SELECT PT2.track_id FROM Playlists_Tracks PT2 WHERE PT2.playlist_id = PT1.playlist_id )
);

-- 9. Provide queries that demonstrates the overlap and covering constraints.
-- The query below demonstrates that there is no overlab between tracks and audiobooks with their ids
-- The result of this query should return no records since they are mutually exclusive while keeping both
-- tables as children of the Audio Table
(SELECT track_id from Track
INTERSECT
SELECT audiobook_id from Audiobook);

-- The query below demosntrates that the Audio table covers the children Track and Audiobook completely.
-- Since Track and Audiobook use foreignkeys, we don't need to worry about the reverse.
-- The database should not have any audio records that do not appear in either Track or Audiobook
SELECT audio_id from Audio
EXCEPT
(
    SELECT track_id FROM Track
    UNION
    SELECT audiobook_id FROM Audiobook
);

-- The complex trigger below 
CREATE TRIGGER TRIGGER_NO_TRACK_AUDIOBOOK_OVERLAP
BEFORE INSERT ON Track
FOR EACH ROW
BEGIN
    DECLARE audiobook_id INT;
    -- Fetch the audiobook_id if exists
    SELECT audiobook_id INTO audiobook_id FROM `Audiobook` WHERE audiobook_id = NEW.track_id;
    
    -- Check if the album market matches the playlist market
    IF audiobook_id IS NOT NULL 
    THEN
        SIGNAL SQLSTATE '45000' 
        SET MESSAGE_TEXT = 'Cannot insert track since it shares same id as the audiobook with id ';
    END IF;
END;