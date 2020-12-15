# Project four : Data Lake Using Spark 
Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

I'll be able to test the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare the results with their expected results.

# Project Scope

In this project, I apply what I've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. in order to finish the project, I will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. I'll deploy this Spark process on a cluster using AWS.

# Data Source 
1. Song datasets: json files saved  in  Amazon S3 Bucket 

2. Log datasets:  json files saved  in  Amazon S3 Bucket 


# list of Dimension Tables and Fact Table
### songplays - Fact table - records in log data associated with song plays i.e. records with page NextSong

- songplay_id (INT) PRIMARY KEY: ID of each user song play
- start_time (DATE) NOT NULL: Timestamp of beggining of user activity
- user_id (INT) NOT NULL: ID of user
- level (TEXT): User level {free | paid}
- song_id (TEXT) NOT NULL: ID of Song played
- artist_id (TEXT) NOT NULL: ID of Artist of the song played
- session_id (INT): ID of the user Session
- location (TEXT): User location
- user_agent (TEXT): Agent used by user to access Sparkify platform 

### users - users in the app

- user_id (INT) PRIMARY KEY: ID of user
- first_name (TEXT) NOT NULL: Name of user
- last_name (TEXT) NOT NULL: Last Name of user
- gender (TEXT): Gender of user {M | F}
- level (TEXT): User level {free | paid}

### songs - songs in music database

- song_id (TEXT) PRIMARY KEY: ID of Song
- title (TEXT) NOT NULL: Title of Song
- artist_id (TEXT) NOT NULL: ID of song Artist
- year (INT): Year of song release
- duration (FLOAT) NOT NULL: Song duration in milliseconds

### artists - artists in music database

- artist_id (TEXT) PRIMARY KEY: ID of Artist
- name (TEXT) NOT NULL: Name of Artist
- location (TEXT): Name of Artist city
- lattitude (FLOAT): Lattitude location of artist
- longitude (FLOAT): Longitude location of artist

### time - timestamps of records in songplays broken down into specific units

- start_time (DATE) PRIMARY KEY: Timestamp of row
- hour (INT): Hour associated to start_time
- day (INT): Day associated to start_time
- week (INT): Week of year associated to start_time
- month (INT): Month associated to start_time
- year (INT): Year associated to start_time
- weekday (TEXT): Name of week day associated to start_time




# Python Packages 
1. Koalas
2. Pyspark
3. configparser
4. os
5. datetime


# Execution Steps 
1. Adding AWS Access info to dl.cfg file 
2. Create an S3 Bucket for tables ourput data 
3. run python etl.py