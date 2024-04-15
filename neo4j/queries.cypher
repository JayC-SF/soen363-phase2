// 1. A basic search query on an attribute value.
// find track with spotify_id 4kY5HMYgRgvHkdGtpdXcAs and return the node.
match (t:Track {spotify_id: '4kY5HMYgRgvHkdGtpdXcAs'}) return t;

// 2. A query that provides some aggregate data (i.e. number of entities satisfying a criteria)
// find the count of tracks with a popularity higher than 65
match (t:Track) where t.popularity > 65 return count(t);

// 3. Find top n entities satisfying a criteria, sorted by an attribute.
// Find the top 5 explicit Audiobooks with the highest number of total chapters in descending order.
match (b:Audiobook) where b.explicit = True return b order by b.total_chapters desc limit 5;

// 4. Simulate a relational group by query in NoSQL (aggregate per category).
// Find all the top 3 genres associated with the highest count of artists having an id greater than 60
// and return the genre, and their artists. 
match (g:Genre)-[:OfGenre]-(a:Artist) where a.artist_id > 60 with g, count(a) as associated_artists_count order by associated_artists_count desc limit 5
return g, associated_artists_count;

// 5. Build the appropriate indexes for previous queries, report the index creation statement and the query execution time before and after you create the index.

// Q1: indexing Track.spotify_id
// 182ms without index
// 1ms with index
create index track_spotify_id for (t:Track) on (t.spotify_id);

// Q2: indexing Track.popularity
// 160 ms without index
// 12 ms with index
create index track_popularity for (t:Track) on (t.popularity);

// Q3: indexing Audiobook.total_chapters and Audiobook.explicit
// 7ms without index
// 1ms with index
create index audiobook_explicit_chapters for (b:Audiobook) on (b.total_chapters, b.explicit);

// Q4
// 115ms without index
// 53ms with index
// note this index already exists in the loader.cypher, therefore it must be dropped before attempting tests 
// without index.
// create index artist_artist_id for (a:Artist) on (a.artist_id);

// 6. Demonstrate a full text search. Show the performance improvement by using indexes.
// query takes 169ms
match (b:Track)
where b.track_name contains 'there'
return count(b);

create fulltext index Track_track_name for (t:Track) on each [t.track_name];
// query takes 35ms
CALL db.index.fulltext.queryNodes("Track_track_name", "there") YIELD node, score
RETURN node, score;



