import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ("""
DROP TABLE IF EXISTS staging_events
""")
staging_songs_table_drop = ("""
DROP TABLE IF EXISTS staging_songs
""")
songplay_table_drop = ("""
DROP TABLE IF EXISTS songplay
""")
user_table_drop = ("""
DROP TABLE IF EXISTS users
""")
song_table_drop = ("""
DROP TABLE IF EXISTS song
""")
artist_table_drop = ("""
DROP TABLE IF EXISTS artist
""")
time_table_drop = ("""
DROP TABLE IF EXISTS time
""")

# CREATE TABLES
"""Creating intermediate events/logs table to perform ETL"""
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
event_id bigint IDENTITY(0,1) not null,
artist varchar null,
auth varchar null,
firstName varchar null,
lastName varchar null,
gender varchar null,
itemInSession varchar null,
length decimal null,
level varchar null,
location varchar null,
method varchar null,
page varchar null,
registration decimal null,
sessionId int not null,
song varchar null,
status int null,
ts bigint not null,
userAgent varchar null,
userId varchar null
);
""")

"""Creating intermediate songs table to perform ETL"""
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs int not null,
artist_id varchar not null,
artist_latitude decimal null,
artist_longitude decimal null,
artist_location varchar null,
artist_name varchar not null,
song_id varchar not null,
title varchar not null,
duration decimal not null,
year int not null
);
""")


""" User: Dimensional table """
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id varchar, 
first_name varchar,
last_name varchar, 
gender varchar, 
level varchar,
PRIMARY KEY(user_id)
);
""")

""" Song: Dimensional table """
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar,
title varchar, 
artist_id varchar, 
year int, 
duration decimal,
PRIMARY KEY(song_id)
)
""")

""" Artist: Dimensional table """
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar,
artist_name varchar, 
artist_latitude decimal, 
artist_longitude decimal,
PRIMARY KEY(artist_id)
)
""")

""" Time: Dimensional table """
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP, 
hour int, 
day int,
week_year int, 
month int, 
year int, 
week_day int,
PRIMARY KEY(start_time)
)
""")

"""Songplay: Dimensional table"""
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay (
songplay_id bigint IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL, 
user_id varchar NOT NULL, 
level varchar, 
song_id varchar,
artist_id varchar,
session_id int NOT NULL,
location text,
user_agent text
);
""")

# STAGING TABLES
"""Copying all the events/logs data to the intermediate table staging_events"""
staging_events_copy = ("""
copy staging_events from '{}'
credentials 'aws_iam_role={}'
json '{}'
compupdate off 
region '{}'
timeformat as 'epochmillisecs'
truncatecolumns
blanksasnull
emptyasnull
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['CLUSTER']['REGION']
)

"""Copying all the songs data to the intermediate table staging_songs"""
staging_songs_copy = ("""
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
format as json 'auto'
compupdate off 
region '{}';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['CLUSTER']['REGION']
)

# FINAL TABLES
"""Insert data from staging_events and staging_songs to the dimensional table below"""
songplay_table_insert = ("""
INSERT INTO songplay (
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
TIMESTAMP 'epoch' + sevent.ts/1000 * INTERVAL '1 second' as start_time,
sevent.userId as user_id,
sevent.level,
ssongs.song_id,
ssongs.artist_id,
sevent.sessionid as session_id,
sevent.location,
sevent.userAgent as user_agent
FROM staging_events sevent
JOIN staging_songs ssongs ON sevent.artist = ssongs.artist_name AND sevent.song = ssongs.title
WHERE sevent.page = 'NextSong'
""")

"""Insert data from staging_events to the fact table below"""
user_table_insert = ("""
INSERT INTO users (
user_id, 
first_name, 
last_name, 
gender, 
level)

SELECT
sevent.userId as user_id,
sevent.firstName as first_name,
sevent.lastName as last_name,
sevent.gender,
sevent.level
FROM staging_events sevent
WHERE sevent.page = 'NextSong'
""")

"""Insert data from staging_songs to the fact table below"""
song_table_insert = ("""
INSERT INTO songs (
song_id, 
title, 
artist_id, 
year, 
duration)
SELECT
ssongs.song_id,
ssongs.title,
ssongs.artist_id,
ssongs.year,
ssongs.duration
FROM staging_songs ssongs; 
""")

"""Insert data from staging_songs to the fact table below"""
artist_table_insert = ("""
INSERT INTO artists (
artist_id, 
artist_name, 
artist_latitude, 
artist_longitude)

SELECT
ssongs.artist_id,
ssongs.artist_name,
ssongs.artist_latitude,
ssongs.artist_longitude
FROM staging_songs ssongs
""")

"""Insert data from staging_events to the fact table below"""
time_table_insert = ("""
INSERT INTO time (
start_time, 
hour, 
day, 
week_year, 
month, 
year, 
week_day)
SELECT 
TIMESTAMP 'epoch' + sevent.ts/1000 * INTERVAL '1 second' as start_time,
EXTRACT(HOUR FROM start_time) as hour,
EXTRACT(DAY FROM start_time) as day,
EXTRACT(WEEK FROM start_time) as week,
EXTRACT(MONTH FROM start_time) as month,
EXTRACT(YEAR FROM start_time) as year,
EXTRACT(DOW FROM start_time) as week_day
FROM staging_events sevent;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create, song_table_create,
    artist_table_create, time_table_create
]
drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
    user_table_drop, song_table_drop, artist_table_drop, time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert,
    artist_table_insert, time_table_insert
]
