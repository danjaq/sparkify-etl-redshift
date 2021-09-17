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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                eventId INT IDENTITY(0,1) PRIMARY KEY,
                                artist TEXT,
                                auth TEXT,
                                firstName TEXT,
                                gender VARCHAR(1),
                                itemInSession INT,
                                lastName TEXT,
                                length TEXT,
                                level TEXT,
                                location TEXT,
                                method TEXT,
                                page TEXT,
                                registration TEXT,
                                sessionId INT,
                                song TEXT,
                                status TEXT,
                                ts TIMESTAMP,
                                userAgent TEXT,
                                userId INT)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs INT,
                                artist_id VARCHAR(18),
                                artist_latitude TEXT,
                                artist_longitude TEXT,
                                artist_location TEXT,
                                artist_name TEXT,
                                song_id VARCHAR(18) PRIMARY KEY,
                                title TEXT,
                                duration FLOAT,
                                year INT)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id int IDENTITY(0,1) PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id int,
                            level text,
                            song_id text,
                            artist_id text,
                            session_id int,
                            location text,
                            user_agent text)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id text PRIMARY KEY,
                        first_name text NOT NULL,
                        last_name text NOT NULL,
                        gender character,
                        level text)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id text PRIMARY KEY,
                        title text NOT NULL,
                        artist_id text NOT NULL,
                        year int,
                        duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artist_id text PRIMARY KEY,
                        name text NOT NULL,
                        location text,
                        latitude numeric,
                        longitude numeric)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int,
                        weekday int)""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                        FROM {} 
                        iam_role {}
                        compupdate off
                        region 'us-west-2'
                        json {}
                        TIMEFORMAT AS 'epochmillisecs'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""COPY staging_songs
                        FROM {} 
                        iam_role {}
                        compupdate off
                        region 'us-west-2'
                        json 'auto ignorecase'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(
                                start_time,
                                user_id,
                                level,
                                song_id,
                                artist_id,
                                session_id,
                                location,
                                user_agent)
                                SELECT se.ts, 
                                    se.userId, 
                                    se.level, 
                                    ss.song_id, 
                                    ss.artist_id,
                                    se.sessionId, 
                                    se.location,
                                    se.userAgent 
                                FROM staging_events AS se
                                JOIN staging_songs AS ss
                                    ON (se.artist = ss.artist_name AND se.song = ss.title)""")

user_table_insert = ("""INSERT INTO users(
                        user_id,
                        first_name,
                        last_name,
                        gender,
                        level)
                    SELECT DISTINCT userId,
                        firstName,
                        lastName,
                        gender,
                        level
                    FROM staging_events
                    WHERE userId IS NOT NULL""")

song_table_insert = ("""INSERT INTO songs(
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration)
                        SELECT DISTINCT song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists(
                            artist_id,
                            name,
                            location,
                            latitude,
                            longitude)
                        SELECT DISTINCT artist_id,
                            artist_name,
                            artist_location,
                            artist_latitude,
                            artist_longitude
                        FROM staging_songs
                        WHERE artist_id IS NOT NULL""")

time_table_insert = ("""INSERT INTO time(
                        start_time,
                        hour,
                        day,
                        week,
                        month,
                        year,
                        weekday)
                        SELECT DISTINCT ts,
                            EXTRACT (hour FROM ts),
                            EXTRACT (day FROM ts),
                            EXTRACT (week FROM ts),
                            EXTRACT (month FROM ts),
                            EXTRACT (year FROM ts),
                            EXTRACT (dayofweek FROM ts)
                       FROM staging_events""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
