import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    hour,
    monotonically_increasing_id,
    month,
    udf,
    weekofyear,
    year,
)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']  # type: ignore
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']  # type: ignore


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = (df_song.select('song_id',
                                'title',
                                'artist_id',
                                'year',
                                'duration')
                        .dropDuplicates(subset=['song_id']))
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table.write
                .partitionBy('year', 'artist_id')
                .parquet(
                    os.path.join(output_data, 'songs'),
                    'overwrite'))

    # extract columns to create artists table
    artists_table = (df_song.select('artist_id',
                                    'name',
                                    'location',
                                    'lattitude',
                                    'longitude')
                            .dropDuplicates(subset=['artist_id']))
    
    # write artists table to parquet files
    (artists_table.write
                  .partitionBy('year', 'artist_id')
                  .parquet(
                      os.path.join(output_data, 'artists'),
                      'overwrite'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df_logs = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_logs = df_logs.filter(df_logs.page == 'NextSong')

    # extract columns for users table    
    users_table = (df_logs.select('userId', 'firstName',
                            'lastName', 'gender',
                            'level')
                     .dropDuplicates(subset=['userId']))

    # write users table to parquet files
    (users_table.write
                .parquet(
                    os.path.join(output_data, 'users'),
                    'overwrite'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(int(ts)/1000)))
    df_logs = df_logs.withColumn('timestamp', get_timestamp(df_logs.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda dt: str(datetime.fromtimestamp(int(dt) / 1000)))
    df_logs = df_logs.withColumn('datetime', get_datetime(df_logs.ts))
    
    # extract columns to create time table
    time_table = (df_logs.select('datetime')
                        .withColumn('start_time', df_logs.datetime)
                        .withColumn('hour', hour('datetime'))
                        .withColumn('day', dayofmonth('datetime'))
                        .withColumn('week', weekofyear('datetime'))
                        .withColumn('month', month('datetime'))
                        .withColumn('year', year('datetime'))
                        .withColumn('weekday', dayofweek('datetime'))
                        .dropDuplicates())
    
    # write time table to parquet files partitioned by year and month
    (time_table.write
               .partitionBy('year', 'month')
               .parquet(os.path.join(output_data, 'time'),
                        'overwrite'))

    # read in song data to use for songplays table
    df_song = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # Join 
    joined_df = df_logs.join(df_song,
                             df_logs.artist == df_song.artist_name,
                             'inner')
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = joined_df.select(
            joined_df.datetime.alias('start_time'),
            joined_df.userId.alias('user_id'),
            joined_df.level.alias('level'),
            joined_df.song_id.alias('song_id'),
            joined_df.artist_id.alias('artist_id'),
            joined_df.sessionId.alias('session_id'),
            joined_df.location.alias('location'), 
            joined_df.userAgent.alias('user_agent'),
            joined_df.datetime.alias('year'),
            joined_df.datetime.alias('month')) \
            .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    (songplays_table.write
                    .partitionBy('year', 'month')
                    .parquet(os.path.join(output_data,
                                          'songplays'),
                             'overwrite'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-bucket-am/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
