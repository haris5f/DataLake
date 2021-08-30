import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import  pyspark.sql.functions as F
import datetime
import pyspark.sql.functions as psf
from pyspark.sql.types import TimestampType as Tst


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    It creates a spark session so that data can be proessded on spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    It takes three inputs. One is a spark cursor. Input_data is a path to the
    song data files on S3. Output Data is a path to the final destination of data.
    This function loads data from S3 and extracts important columns to be put in analytics 
    tables. Data is stored back on S3
    """
        
        
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("songs")
    songs_table = spark.sql("""
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM songs
        WHERE song_id IS NOT NULL
        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year","artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT 
            DISTINCT artist_id, artist_name AS name, artist_location AS location, 
            artist_latitude AS   latitude , artist_longitude AS longitude 
        FROM songs
        WHERE artist_id IS NOT NULL
        """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    """
    This function takes same inputs to that of process_song_data function.
    This function loads data from S3 and extracts important columns to be put in analytics 
    tables. Data is stored back on S3
    """
    
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    df_log.createOrReplaceTempView("log")
    df_users = spark.sql("""
        SELECT DISTINCT userId as user_id, firstName as first_name,
        lastName as last_name, gender, level
        FROM log
        """)
    
    # write users table to parquet files
    df_users.write.mode("overwrite").parquet(output_data + "users")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000),Tst())
    df_log = df_log.withColumn("datetime", get_timestamp(df_log.ts))
    
  
    # extract columns to create time table
    df_log.createOrReplaceTempView("timetable")
    df_time = spark.sql("""
        SELECT 
            DISTINCT datetime as start_time, 
            EXTRACT(HOUR FROM datetime) as hour,
            EXTRACT(DAY FROM datetime) as day,
            EXTRACT(WEEK FROM datetime) as week,
            EXTRACT(MONTH FROM datetime) as month,
            EXTRACT(YEAR FROM datetime) as year,
            EXTRACT(DAYOFWEEK FROM datetime) as weekday
        FROM timetable

        """)
    
    # write time table to parquet files partitioned by year and month
    df_time.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songs")
    df_songplays = spark.sql("""
        SELECT
            t.datetime as start_time,
            t.userId, t.level, s.song_id, s.artist_id,
            t.sessionId, s.artist_location, t.userAgent,
            EXTRACT(MONTH FROM t.datetime) as month,
            EXTRACT(YEAR FROM t.datetime) as year
        FROM songs s,timetable t
        WHERE t.artist = s.artist_name
        AND t.song = s.title
        """) 
    df_songplays = df_songplays.withColumn('songplay_id', psf.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    df_songplays.write.mode("overwrite").partitionBy("year","month").parquet(output_data + "songplays/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://mynewlakebucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    spark.stop()


if __name__ == "__main__":
    main()
