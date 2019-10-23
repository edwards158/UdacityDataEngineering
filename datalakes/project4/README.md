## Project: Data Lakes

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build a ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. Test the database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description
Using Spark and data lakes build an ETL pipeline for a data lake hosted on S3. To complete the project, load data from S3, process the data into analytics tables using Spark, and load them back into S3. Deploy this Spark process on a cluster using AWS.

## Project Steps

## 1 Create Table Schemas
Using the song and event datasets, the following tables were created.

### Fact Table
1. **songplays** - records in event data associated with song plays i.e. records with page NextSong 
    - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*
   
### Dimension Tables
2. **users** - users in the app
    - *user_id, first_name, last_name, gender, level*
3. **songs** - songs in music database
    - *song_id, title, artist_id, year, duration*
4. **artists** - artists in music database
    - *artist_id, name, location, lattitude, longitude*
5. **time** - timestamps of records in songplays broken down into specific units
    - *start_time, hour, day, week, month, year, weekday*


Using the file **etl.py**:
- Load in song dataset from aws s3.  Using a spark cluster in aws spark process the data to produce a song and artists tables.
- Write these table to parquet files back to s3.
- Load in log dataset from aws s3.  Using spark cluster in aws the data to produce a user, time and songplays tables.
- Write these table to parquet files back to s3.


**etl.ipnyb** was used on a subset of the data and processed locally on spark to reduce develoment time on the full dataset