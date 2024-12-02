"""This is for the SQL queries of creating, inserting data into the tables. No need to run it as the other 2 files import variables from this file."""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ARN= config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES
"""sql for dropping tables in case any exists"""
staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists fact_songplay"
user_table_drop = "drop table if exists dim_user"
song_table_drop = "drop table if exists dim_song"
artist_table_drop = "drop table if exists dim_artist"
time_table_drop = "drop table if exists dim_time"

# CREATE TABLES
"""sql for create the tables"""
staging_events_table_create= ("""
create table staging_events
(artist varchar,
auth varchar,
firstname varchar,
gender varchar,
iteminsession int,
lastname varchar,
length varchar,
level varchar,
location varchar,
method varchar,
page varchar,
registration decimal,
sessionid int,
song varchar,
status int,
ts bigint,
useragent varchar,
userid int
)

""")

staging_songs_table_create = ("""
create table staging_songs
(
num_songs int,
artist_id varchar,
artist_latitude varchar,
artist_longitude varchar,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration decimal,
year int
)
""")

user_table_create = ("""
create table dim_user
(
user_id int,
first_name varchar,
last_name varchar,
gender varchar,
level varchar,
primary key(user_id))
diststyle all
sortkey (user_id)
""")

song_table_create = ("""
create table dim_song
(
song_id varchar,
title varchar,
artist_id varchar,
year int,
duration int,
primary key(song_id),
foreign key(artist_id) references dim_artist(artist_id)
)
diststyle all
sortkey (song_id)
""")

artist_table_create = ("""
create table dim_artist(
artist_id varchar,
name varchar,
location varchar,
latitude varchar,
longitude varchar,
primary key(artist_id)
)
diststyle all
sortkey (artist_id)
""")

time_table_create = ("""
create table dim_time 
(
start_time timestamp,
hour int not null,
day int not null,
week int not null,
month int not null,
year int not null,
weekday int not null,
primary key(start_time)
)
diststyle all
sortkey (start_time)
""")


songplay_table_create = ("""
create table fact_songplay
(
songplay_id bigint identity(0, 1),
start_time timestamp,
user_id int,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar,
primary key(songplay_id),
foreign key(user_id) references dim_user(user_id),
foreign key(song_id) references dim_song(song_id),
foreign key(start_time) references dim_time(start_time),
foreign key(artist_id) references dim_artist(artist_id))
distkey(songplay_id)
sortkey(start_time);
""")

# STAGING TABLES
"""sql for copy data from s3 to redshift"""
staging_events_copy = ("""
copy staging_events from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' json {};
""").format(LOG_DATA, IAM_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2' json 'auto';
""").format(SONG_DATA, IAM_ARN)

# FINAL TABLES

songplay_table_insert = ("""
insert into fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
select 
CAST(DATEADD(SECOND, ts/1000,'1970/1/1') AS timestamp) as start_time,
a.userid as user_id,
a.level,
b.song_id as song_id,
b.artist_id as artist_id,
a.sessionid as session_id,
a.location as location,
trim(both '"' from a.useragent) as user_agent
from staging_events a
left join staging_songs b on a.song = b.title and a.artist = b.artist_name
where a.page = 'NextSong'
group by 1,2,3,4,5,6,7,8;
""")

user_table_insert = ("""
insert into dim_user (user_id, first_name, last_name, gender, level) 
select userid as user_id, firstname as first_name, lastname as last_name, gender, level
from staging_events
where page = 'NextSong'
group by 1,2,3,4,5;
""")

song_table_insert = ("""
insert into dim_song (song_id, title, artist_id, year, duration) 
select song_id, title, artist_id, year, duration
from staging_songs
group by 1,2,3,4,5;
""")

artist_table_insert = ("""
insert into dim_artist (artist_id, name, location, latitude, longitude) 
select artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude
from staging_songs
group by 1,2,3,4,5;
""")

time_table_insert = ("""
insert into dim_time (start_time, hour, day, week, month, year, weekday) 
select CAST(DATEADD(SECOND, ts/1000,'1970/1/1') AS timestamp) as start_time,
extract(hour from start_time) as hour,
extract(day from start_time) as day,
extract(week from start_time) as week,
extract(month from start_time) as month,
extract(year from start_time) as year,
extract(weekday from start_time) as weekday
from staging_events
where page = 'NextSong'
group by 1,2,3,4,5,6,7;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
