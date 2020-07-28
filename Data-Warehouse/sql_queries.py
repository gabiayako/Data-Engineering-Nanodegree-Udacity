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
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
            artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration numeric,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time bigint NOT NULL,
        user_id int NOT NULL,
        level varchar,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude numeric,
        longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar
    )
""")

# STAGING TABLES

LOG_DATA_FOLDER = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA_FOLDER = config.get("S3","SONG_DATA")
DWH_ROLE_ARN = config.get("IAM_ROLE","ARN")

staging_events_copy = (
"""
    COPY staging_events FROM {}
    iam_role {}
    json {}
    compupdate off region 'us-west-2';
""".format(LOG_DATA_FOLDER, DWH_ROLE_ARN, LOG_JSONPATH)
)


staging_songs_copy = """
    COPY staging_songs FROM {}
    iam_role {}
    json 'auto' compupdate off region 'us-west-2';
""".format(SONG_DATA_FOLDER, DWH_ROLE_ARN)

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
        e.ts,
        e.userId,
        e.level,
        s.song_id,
        a.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events as e
    LEFT JOIN songs as s on e.song = s.title
    LEFT JOIN artists as a on e.artist = a.name
    WHERE 
        e.ts is not null and
        e.userId is not null and
        s.song_id is not null and
        a.artist_id is not null
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender,level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE userId is not null;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
    FROM staging_songs;
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
        ts, 
        EXTRACT(HOUR FROM ts),
        EXTRACT(DAY FROM ts),
        EXTRACT(WEEK FROM ts),
        EXTRACT(MONTH FROM ts),
        EXTRACT(YEAR FROM ts),
        EXTRACT(WEEKDAY FROM ts)
    FROM( 
        SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ') as ts FROM staging_events
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
