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
    CREATE TABLE IF NOT EXISTS staging_events 
    (
        artist text,
        author text,
        first_name text,
        gender varchar(1),
        item_in_session int,
        last_name text,
        length numeric,
        level text,
        location text,
        method text,
        page text,
        registration numeric,
        session_id int,
        song text,
        status int,
        ts bigint,
        user_agent text,
        user_id int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs int,
        artist_id text,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_name text,
        song_id text,
        title text,
        duration numeric,
        year int
    )
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL SORTKEY DISTKEY,
        user_id int NOT NULL,
        level text,
        song_id text NOT NULL,
        artist_id text NOT NULL,
        session_id int,
        location text
        )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id int NOT NULL SORTKEY PRIMARY KEY,
        first_name text NOT NULL,
        last_name text NOT NULL,
        gender varchar(1),
        level text
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id text NOT NULL SORTKEY PRIMARY KEY,
        title text NOT NULL,
        artist_id text NOT NULL,
        year int NOT NULL,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id text PRIMARY KEY,
        name text,
        location text,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp NOT NULL DISTKEY SORTKEY PRIMARY KEY,
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int  NOT NULL
     )
""")


staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {}
    region 'us-west-2'
    json {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'),
config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role {}
    region 'us-west-2'
    JSON 'auto' ;
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO 
        songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  
        timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.user_id, se.level, 
        ss.song_id, ss.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events se, staging_songs ss
    WHERE se.page = 'NextSong' AND
    se.song =ss.title AND
    se.artist = ss.artist_name AND
    se.length = ss.duration
""")

user_table_insert = ("""
    INSERT INTO 
        users(user_id, first_name, last_name, gender, level)
    SELECT distinct  
        user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO 
        songs(song_id, title, artist_id, year, duration)
    SELECT 
        song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO 
        artists(artist_id, name, location, latitude, longitude)
    SELECT distinct 
        artist_id, artist_name, artist_location , artist_latitude, artist_longitude 
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO 
        time(start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, extract(hour from start_time), extract(day from start_time),
        extract(week from start_time), extract(month from start_time),
        extract(year from start_time), extract(dayofweek from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
