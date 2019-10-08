# Project: Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim was to create Postgres database with tables designed to optimize queries on song play analysis.  This required a database schema and ETL pipeline for this analysis. The database and ETL pipeline were to be tested by running queries given to me by the analytics team from Sparkify so as to compare my results with their expected results.

## Project Description
In this project I built a ETL pipeline using Python. To complete the project, fact and dimension tables were defined with a star schema for a particular analytic focus and ETL pipeline was created that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Project Files and Folders

* *SQLrelation.png* - a SQL relationship diagram of the constructed star schema.
* *data* - folder containing the song and user activity files in json format.
* *create_tables.py* - drops and creates tables. Run this file to reset the tables.

* *etl.ipynb* - reads and processes a single file from song_data and log_data and loads the data into the tables. Contains detailed instructions on the ETL process for each of the tables.
* *etl.py* - reads and processes files from song_data and log_data and loads them into the tables.
* *sql_queries.py* - contains all the sql queries, and is imported into the last three files above.
* *README.md* - provides discussion on the project.