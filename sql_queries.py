import configparser
from psycopg2.sql import Literal


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
) DISTSTYLE KEY DISTKEY (songplay_id);
""")

users_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR NOT NULL,
    level VARCHAR NOT NULL
) DISTSTYLE KEY DISTKEY (user_id);
""")

songs_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INTEGER NOT NULL,
    duration DECIMAL NOT NULL
) DISTSTYLE KEY DISTKEY (song_id);
""")

artists_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    latitude DECIMAL NOT NULL,
    longitude DECIMAL NOT NULL
) DISTSTYLE KEY DISTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INTEGER NOT NULL,
    day INTEGER NOT NULL,
    week INTEGER NOT NULL,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL,
    weekday INTEGER NOT NULL
) DISTSTYLE KEY DISTKEY (start_time);
""")

# STAGING TABLES
# COPY staging_events table

# staging_events_copy = ("""
#     COPY staging_events FROM 's3://udacity-dend/log_data'
#     IAM_ROLE {}
#     JSON 's3://udacity-dend/log_json_path.json'
#     region 'us-west-2';
# """).format(ARN)

# staging_songs_copy = ("""
#     COPY staging_songs FROM 's3://udacity-dend/song_data'
#     IAM_ROLE {}
#     REGION 'us-west-2'
#     JSON 'auto';
# """.format(ARN))
# secrect key account AKIAUQU33UKLCJKILP3S
# access key account  cCnd7jxGkuJVxWU3PHHJSWPeSVXj7WDpPWZn+mow
# db access awsuser
# db pass Vinhday123!
# db name dev
# port 5439
staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data/2018'
    IAM_ROLE {}
    JSON 's3://udacity-dend/log_json_path.json'
    region 'us-west-2';
""").format(ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data/A/A'
    IAM_ROLE {}
    REGION 'us-west-2'
    JSON 'auto';
""".format(ARN))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    TO_CHAR(to_timestamp(e.ts / 1000), 'DD/MM/YYYY HH24:MI:SS') AS start_time,
    e.userId AS user_id,
    e.level AS level,
    s.song_id AS song_id,
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s
    ON e.artist = s.artist_name
    AND e.length = s.duration
    AND e.song = s.title;
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender AS gender,
    level AS level
FROM staging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id AS song_id,
    title AS title,
    artist_id AS artist_id,
    year AS year,
    duration AS duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    artist_id AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    ts AS start_time,
    EXTRACT(HOUR FROM ts) AS hour,
    EXTRACT(DAY FROM ts) AS day,
    EXTRACT(WEEK FROM ts) AS week,
    EXTRACT(MONTH FROM ts) AS month,
    EXTRACT(YEAR FROM ts) AS year,
    EXTRACT(DOW FROM ts) AS weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

