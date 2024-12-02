# Project: Data Warehouse
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and
want to move their processes and data onto the cloud. Their data resides in S3, 
in a directory of JSON logs on user activity on the app, as well as a directory 
with JSON metadata on the songs in their app.

This goal of this project is to build an ETL pipeline that extracts their data from S3,
stages them in Redshift, and transforms data into a set of dimensional tables for 
their analytics team to continue finding insights into what songs their users are 
listening to.

## Data Warehose Design
The process of building the data warehouse include two steps: 
1. build ETL pipeline to load data from AWS s3 storage into Redshift staging tables
2. transform staging tables into star schema and load the transformed data into final redshift tables for analytical use.

### Load data from s3 to redshift
The JSON files in s3 contain events and songs data. Events data logs the song listening activity. 
Songs data includes song and artist information. The ETL pipeline focus on copy the data 
from s3 and directly load into staging tables without schema or format changes. 

#### Staging tables: 
1. staging_events 
2. staging_songs

### Transform staging tables into star schema
To enable easy access and understanding of the tables for analysts, star schema is utilized for the 
final tables. The tables contain:
#### fact table:
fact_songplay - Table contains the song listening events by users. It includes the 
ids of user, song, artist, session, time. Analyst can join to the dim tables for more details 
of each demension.
#### dimension tables:
   1. dim_user - table contains user details including name, gender and level
   2. dim_song - table contains song details including title, artist, year and duration
   3. dim_artist - table contains artist details including name, location, lat and long
   4. dim_time - table contains details of the time when user listen to the song such as hour, day and weekday etc.

## How to use the code
The code contains:
1. create_tables.py - Run this code for creating the staging and final tables.
2. etl.py - Run this code for etl pipelines to load data into staging and final tables
3. sql_queries.py - This is for the SQL queries of creating, inserting data into the tables. No need to run it as the other 2 files import variables from this file.
4. dwh.cfg - the file contains the constant variables of redshift cluster, s3 and IAM role. The .py files contain code to read from this file. 


## Sample Query
### Num of song play records
`select count(*) from fact_songplay;`
### Num of songs played by user name
`select b.first_name, b.last_name, count(distinct a.songplay_id) as song_plays
from fact_songplay a
join dim_user b on a.user_id = b.user_id
group by b.first_name, b.last_name;`

### Num of songs played by week
`select b.week, count(distinct a.songplay_id) as song_plays
from fact_songplay a
join dim_time b on a.start_time = b.start_time
group by 1;`