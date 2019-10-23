import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
import pandas as pd
from pyspark.sql.functions import monotonically_increasing_id
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """ Process song data and write songs and artists tables"""
    
    print("------------processing songs----------\n")

    #use the full data set
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    #use a subset of the dataset
    #song_data = os.path.join(input_data,"song_data/A/A/A/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration']
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,"songs.parquet"),'overwrite')
    
    print("------------processing artists----------\n")
    
    # extract columns to create artists table
    artists_table = df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    
    #artists_table.limit(2).toPandas()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists.parquet"),'overwrite')


def process_log_data(spark, input_data, output_data):
    
    """ Process log data and write users, time and songplays tables"""
    
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")

    #use a subset of the dataset
    #log_data = '/home/workspace/data/log-data/2018-11-19-events.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']
    
    print("------------processing users----------\n")

    # extract columns for users table    
    users_table = df[['userId','firstName', 'lastName','gender','level']]

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"users.parquet"),'overwrite')
    
    print("------------processing time----------\n")
    
    #add new column for timestamp
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    df = df.withColumn("start_time", df.timestamp)
    
    df = df.withColumn('hour',hour('start_time')).withColumn('day',dayofmonth('start_time')).withColumn('week',weekofyear('start_time'))
    df = df.withColumn('month',month('start_time')).withColumn('year',year('start_time')).withColumn('weekday',dayofweek('start_time'))
    
    # extract columns to create time table
    time_table = df[['start_time','hour','day','week','month','year','weekday']]

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,"time.parquet"),'overwrite')
    
    print("------------processing songplays----------\n")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs.parquet")
    
    df = df.join(song_df,song_df.title == df.song)
    songplays_table = df['start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data) 
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
