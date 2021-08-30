# Project: Data Warehouse
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Summary

In this project, I built a data lake and an ETL pipeline in Spark that loads data from S3, processes the data into analytics tables, and loads them back into S3.

## Datasets

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`
## Schema for Song Play Analysis

Using the song and event datasets, I created a star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table

- songplays - records in event data associated with song plays i.e. records with page NextSong
    > songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

- users - users in the app
    > user_id, first_name, last_name, gender, level
- songs - songs in music database
    > song_id, title, artist_id, year, duration
- artists - artists in music database
    > artist_id, name, location, lattitude, longitude
- time - timestamps of records in songplays broken down into specific units
    > start_time, hour, day, week, month, year, weekday
    
## Project Template

The project template includes three files:

- `etl.py` is where I create an ETL pipeline by loading data from S3 to process it in spark and then loading that data back to S3 for analytical purposes.
- `dl.cfg` is a configuration file containing AWS credentials.
- `README.md` discussion on processes and decisions for this ETL pipeline

## Running the Project

- Add credentials role info to `dl.cfg`
- Connect SSH link to master node on EMR cluster
- Create pipeline by running `etl.py` using `spark-submit etl.py`
