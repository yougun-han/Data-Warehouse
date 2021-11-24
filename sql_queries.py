import configparser


# READ ETL FILE/DATABASE LOCATION AND CREDENTIALS.
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')


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
    CREATE TABLE IF NOT EXISTS stage_events
    (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC, 
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR, 
        title VARCHAR, 
        duration NUMERIC,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY , 
        start_time TIMESTAMP NOT NULL REFERENCES time(start_time), 
        user_id INT NOT NULL REFERENCES users(user_id) distkey, 
        level VARCHAR, 
        song_id VARCHAR NOT NULL REFERENCES songs(song_id), 
        artist_id VARCHAR NOT NULL REFERENCES artists(artist_id) sortkey, 
        session_id INT, 
        location VARCHAR, 
        user_agent VARCHAR,
        UNIQUE (start_time, user_id, song_id, artist_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id INT PRIMARY KEY sortkey distkey, 
        first_name VARCHAR NOT NULL, 
        last_name VARCHAR NOT NULL, 
        gender VARCHAR, 
        level VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id VARCHAR PRIMARY KEY sortkey, 
        title VARCHAR NOT NULL, 
        artist_id VARCHAR NOT NULL, 
        year INT, 
        duration NUMERIC
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id VARCHAR PRIMARY KEY sortkey,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP PRIMARY KEY sortkey,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
    diststyle all;
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY stage_events FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto'
""").format(SONG_DATA, ARN)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time, 
        se.userId       AS user_id,
        se.level        AS level,
        ss.num_songs    AS song_id,
        ss.artist_id    AS artist_id,
        se.sessionId    AS session_id, 
        se.location     AS location, 
        se.userAgent    AS user_agent
    FROM stage_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong' ; 
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT se.userId      AS user_id,
        se.firstName            AS first_name, 
        se.lastName             AS last_name,
        se.gender               AS gender,
        se.level                AS level
    FROM stage_events se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT ss.num_songs        AS song_id,
        ss.title                     AS title,    
        ss.artist_id                 AS artist_id,
        ss.year                      AS year,
        ss.duration                  AS duration
    FROM staging_songs ss
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT
        DISTINCT ss.artist_id   AS artist_id,
        ss.artist_name          AS name,
        ss.artist_location      AS location,    
        ss.artist_latitude      AS latitude,
        ss.artist_longitude     AS longitude
    FROM staging_songs ss
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        DISTINCT songplays.start_time       AS start_time,
        EXTRACT(HOUR FROM start_time)       AS hour,
        EXTRACT(DAY FROM start_time)        AS day,
        EXTRACT(WEEK FROM start_time)       AS week,
        EXTRACT(MONTH FROM start_time)      AS month,
        EXTRACT(YEAR FROM start_time)       AS year,
        EXTRACT(weekday FROM start_time)    AS weekday
    FROM songplays;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
