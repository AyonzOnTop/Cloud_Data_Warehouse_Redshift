import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time_range;"

# CREATE TABLES
#Staging tables

staging_events_table_create= ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR(255),
    auth VARCHAR(50),
    firstName VARCHAR(255),
    gender VARCHAR(1),
    itemsession INTEGER,
    lastname VARCHAR(255),
    length DOUBLE PRECISION,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    sessionid BIGINT,
    song VARCHAR(255),
    status INTEGER,
    ts VARCHAR(50),
    useragent TEXT,
    userid VARCHAR(255)
    
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id VARCHAR(100) PRIMARY KEY,
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    num_songs INTEGER,
    artist_id VARCHAR(255),
    artist_name VARCHAR(255),
    artist_longitude DOUBLE PRECISION,
    artist_latitude DOUBLE PRECISION,
    artist_location VARCHAR(255)
    
)

""")

#FACTS & DIMENSION TABLE

time_table_create = ("""
CREATE TABLE time_range (
    start_time TIMESTAMP PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER

);
""")



user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR(100) PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(50)

);
""")



song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR(100) NOT NULL PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL REFERENCES artists(artist_id) sortkey, 
    year VARCHAR(100),
    duration DOUBLE PRECISION

);
""")



artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION

);
""")


songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP REFERENCES time_range(start_time) sortkey,
    user_id VARCHAR(50) REFERENCES users(user_id),
    level VARCHAR(50),
    song_id VARCHAR(100) REFERENCES songs(song_id),
    artist_id VARCHAR(100) REFERENCES artists(artist_id),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT

);
""")





# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}'
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto'
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])


# FINAL TABLES
# INSERTION INTO FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT 
        TIMESTAMP 'epoch' + e.ts/1000 *INTERVAL '1 second' AS start_time, --e.ts AS start_time,
        e.userid,
        e.level, 
        s.song_id, 
        s.artist_id, 
        e.sessionid, 
        e.location, 
        e.useragent
FROM staging_events e 
JOIN staging_songs s
ON s.title = e.song
WHERE e.page='NextSong'
AND userid NOT in (SELECT DISTINCT s.user_id
                    FROM songplay s
                    WHERE s.user_id = user_id
                    AND s.start_time = start_time
                    AND s.session_id = session_id)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, 
        firstName, 
        lastname, 
        gender, 
        level

FROM staging_events e
WHERE e.page='NextSong'
AND userid NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, 
        title, 
        artist_id, 
        year, 
        duration
FROM staging_songs
WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)

""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT artist_id, 
        artist_name, 
        artist_location, 
        artist_latitude, 
        artist_longitude
FROM staging_songs
WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time_range (start_time, hour, day, week, month, year, weekday)
SELECT start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year,
        EXTRACT(weekday from start_time) AS weekday
FROM (SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time
        FROM staging_events s)
WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time_range)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
