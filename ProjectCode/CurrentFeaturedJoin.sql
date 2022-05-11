
USE `CurrentSpotifyFeatured`;

CREATE TABLE `CurrentSpotifyFeatured`.`currentFeatured`(
`ID` INT(11) NOT NULL AUTO_INCREMENT,
`playlist_id` text,
`playlist_name` text,
`playlist_description` text,
`playlist_image_url` text,
`featured_dt` text,
`track_key` bigint NOT NULL,
`track_id` text,
`track_name` text,
`track_artists` text,
`track_album` text,
`track_date_added` text,
`track_duration_ms` bigint DEFAULT NULL,
`Danceability` double DEFAULT NULL,
`Energy` double DEFAULT NULL,
`Key` bigint DEFAULT NULL,
`Loudness` double DEFAULT NULL,
`Speechiness` double DEFAULT NULL,
`Acousticness` double DEFAULT NULL,
`Instrumentalness` double DEFAULT NULL,
`Liveness` double DEFAULT NULL,
`Valence` double DEFAULT NULL,
`Tempo` double DEFAULT NULL,
`Explicit` tinyint(1) DEFAULT NULL,
`Popularity` bigint DEFAULT NULL,
`track_negative_sentiment` double DEFAULT NULL,
`track_neutral_sentiment` double DEFAULT NULL,
`track_positive_sentiment` double DEFAULT NULL,
`track_compound_sentiment` double DEFAULT NULL,
PRIMARY KEY (`ID`)
)
ENGINE = InnoDB
CHARSET = utf8mb4;


INSERT INTO `CurrentSpotifyFeatured`.`currentFeatured`
(`playlist_id`,
`playlist_name`,
`playlist_description`,
`playlist_image_url`,
`featured_dt`,
`track_key`,
`track_id`,
`track_name`,
`track_artists`,
`track_album`,
`track_date_added`,
`track_duration_ms`,
`Danceability`,
`Energy`,
`Key`,
`Loudness`,
`Speechiness`,
`Acousticness`,
`Instrumentalness`,
`Liveness`,
`Valence`,
`Tempo`,
`Explicit`,
`Popularity`,
`track_negative_sentiment`,
`track_neutral_sentiment`,
`track_positive_sentiment`,
`track_compound_sentiment`)
SELECT p.playlist_id,
p.playlist_name,
p.playlist_description,
p.playlist_image_url,
p.featured_dt,
t.track_key,
t.track_id,
t.track_name,
t.track_artists,
t.track_album,
t.track_date_added,
t.track_duration_ms,
ta.Danceability,
ta.Energy,
ta.Key,
ta.Loudness,
ta.Speechiness,
ta.Acousticness,
ta.Instrumentalness,
ta.Liveness,
ta.Valence,
ta.Tempo,
t.Explicit,
t.Popularity,
ts.track_negative_sentiment,
ts.track_neutral_sentiment,
ts.track_positive_sentiment,
ts.track_compound_sentiment
FROM currentFeaturedTracks AS t
INNER JOIN currentFeaturedPlaylists AS p
ON t.playlist_id = p.playlist_id
RIGHT OUTER JOIN currentFeaturedTrackAnalysis AS ta
ON ta.track_id = t.track_id
INNER JOIN currentFeaturedTrackLyricSentiment AS ts
ON ts.track_id = t.track_id;