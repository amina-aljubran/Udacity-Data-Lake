import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# Read configuration data from dl.cfg
config = configparser.ConfigParser()
config.read('dl.cfg')

# Modify the values of environment variable  
os.environ['AWS_ACCESS_KEY_ID'] = config['ACCESS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['ACCESS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session(): 
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Function to collect song data from ".json" files then store them.
    Parameters:
        - spark: Contains the spark session
        - input_data: Link to read data from
        - output_data: Link to store data in
    Outputs:
        None
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = spark.read.json(song_data)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table.write.partitionBy('year', 'artist_id').\
                    parquet(os.path.join(output_data, 'songs'), 'overwrite')
    
    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', \
                             'artist_latitude', 'artist_longitude']
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Function to collect log data from ".json" files then store them.
    Also, create songplays table (Fact Table)
    Parameters:
        - spark: Contains the spark session
        - input_data: Link to read data from
        - output_data: Link to store data in
    Outputs:
        None
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df[log_df.page == 'NextSong']

    # extract columns for users table    
    users_table = log_df['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(int(x)/1000)))
    log_df = log_df.withColumn('datetime', get_datetime(log_df.ts))
   
    # extract columns to create time table
    time_table = log_df.select('start_time', 
                                hour('datetime').alias('hour'), 
                                dayofmonth('datetime').alias('day'), 
                                weekofyear('datetime').alias('week'), 
                                month('datetime').alias('month'), 
                                year('datetime').alias('year'), 
                                date_format('datetime','u').alias('weekday')
                               ).distinct()
 
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').\
                parquet(os.path.join(output_data, 'time'), 'overwrite')

    # read song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data,"song_data/*/*/*/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = log_df.join(
                                   (log_df.song == song_df.title) & \
                                   (log_df.artist == song_df.artist_name) &\
                                   (log_df.length == song_df.duration),\
                                   left_outer\
                                  ).select(
                                           log_df.timestamp,
                                           userId,
                                           log_df.level,
                                           song_df.song_id,
                                           song_df.artist_id,
                                           sessionId,
                                           log_df.location,
                                           useragent,
                                           year(datetime).alias(year),
                                           month(datatime).alias(month)
                                         )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/" # Read the data from this path
    output_data = "s3://aws-logs-343507596300-us-west-2/elasticmapreduce/" # Store the data in this path
    
    # Read (song and log data), modify, and store
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
