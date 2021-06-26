import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events ( 
artist text, auth text, firstName text, gender text, ItemInSession int, 
lastName text, length float8, level text, location text, method text, 
page text, registration varchar, sessionId int, song text, status int, 
ts bigint, userAgent text, userId text);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
song_id text PRIMARY KEY, artist_id text, artist_latitude float,
artist_longitude float, artist_location text, artist_name text,
duration float, num_songs int, title text, year int);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int PRIMARY KEY IDENTITY(0, 1),
    start_time timestamp NOT NULL SORTKEY,
    user_id text NOT NULL DISTKEY,
    level text,
    song_id text NOT NULL,
    artist_id text NOT NULL,
    session_id int NOT NULL,
    location varchar NOT NULL,
    user_agent text NOT NULL
    ) diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id text NOT NULL PRIMARY KEY DISTKEY, 
    first_name varchar(45), 
    last_name varchar(45), 
    gender varchar(1),
    level text NOT NULL)
    diststyle key;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text NOT NULL PRIMARY KEY,
    title varchar,
    artist_id text DISTKEY,
    year int,
    duration float)
    diststyle key;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id text NOT NULL PRIMARY KEY, 
    name text, 
    location text, 
    latitude float, 
    longitude float)
    diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL PRIMARY KEY DISTKEY SORTKEY, 
    hour varchar, 
    day varchar,
    week varchar,
    month varchar,
    year varchar,
    weekday varchar)
    diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE '{}'
    JSON {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
   COPY staging_songs FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
     timestamp 'epoch' + u.ts * interval '0.001 seconds' as start_time,
     u.userId as user_id,
     u.level as level,
     s.song_id as song_id,
     s.artist_id as artist_id,
     u.sessionId as session_id,
     u.location as location,
     u.userAgent as user_agent
FROM staging_events as u
JOIN staging_songs as s
on u.artist = s.artist_name
AND u.song = s.title
AND u.length = s.duration
WHERE u.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users 
WITH uniq_staging_events AS (
    SELECT userId, firstName, lastName, gender, level,
        ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
    FROM staging_events
    WHERE userId IS NOT NULL)
SELECT 
    userId,
    firstName as first_name,
    lastName as last_name,
    gender,
    level
FROM uniq_staging_events
WHERE rank=1;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT DISTINCT (song_id)
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists SELECT DISTINCT (artist_id)
    artist_id,
    artist_name as name,
    artist_location as location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time 
    WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
        SELECT DISTINCT
        ts,
        extract(hour from ts),
        extract(day from ts),
        extract(week from ts),
        extract(month from ts),
        extract(year from ts),
        extract(weekday from ts)
        FROM temp_time
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
