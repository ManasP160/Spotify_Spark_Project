CREATE DATABASE spotify_db ; 

CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::982081083723:role/spotify-spark-snowflake-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-manas')
    COMMENT = 'Creating connection to S3';


DESC integration s3_init ; 

CREATE OR REPLACE FILE FORMAT csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = TRUE ;  


CREATE OR REPLACE stage spotify_stage
    URL = 's3://spotify-etl-project-manas/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat


LIST @spotify_stage/songs ; 


CREATE OR REPLACE TABLE tbl_album (
    album_id STRING,
    name STRING, 
    release_date DATE, 
    total_tracks INT,
    url STRING
); 

CREATE OR REPLACE TABLE tbl_artists (
    artist_id STRING, 
    name STRING,
    url STRING
);


CREATE OR REPLACE TABLE tbl_songs (
    song_id STRING, 
    song_name STRING,
    duration_ms INT,
    url STRING, 
    popularity INT, 
    song_added DATE,
    album_id STRING, 
    artist_id STRING
);



COPY INTO tbl_songs
FROM @spotify_stage/songs_data/song_transformed_20241019/run-1729372997066-part-r-00001

SELECT * FROM tbl_songs ; 



COPY INTO tbl_artists
FROM @spotify_stage/artist_data/artist_transformed_20241019/run-1729372995588-part-r-00000

SELECT * FROM tbl_artists; 


COPY INTO tbl_album
FROM @spotify_stage/album_data/album_tansforment_20241019/run-1729372994266-part-r-00001

SELECT * FROM tbl_album; 

-- Creating SNOW PIPE
CREATE OR REPLACE SCHEMA pipe ; 

CREATE OR REPLACE PIPE spotify_db.pipe.tbl_songs_pipe
AUTO_INGEST = TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/song_data;


CREATE OR REPLACE PIPE spotify_db.pipe.tbl_artists_pipe
AUTO_INGEST = TRUE 
AS 
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist_data; 

CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE 
AS 
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album_data; 




DESC pipe pipe.tbl_album_pipe; 


SELECT COUNT(*) FROM tbl_songs; 

SELECT * FROM tbl_songs;


SELECT SYSTEM$PIPE_STATUS('pipe.tbl_album_pipe')








