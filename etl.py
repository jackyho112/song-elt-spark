import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, StructType, StringType, IntegerType, FloatType, StructField

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']

# Make a staging song schema to facilitate the reading process
staging_song_schema = StructType([
    StructField("num_songs", IntegerType()),
    StructField("artist_id", StringType()),
    StructField("artist_latitude", FloatType()),
    StructField("artist_longitude", FloatType()),
    StructField("artist_location", StringType()),
    StructField("artist_name", StringType()),
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("duration", FloatType()),
    StructField("year", IntegerType()),
])

def create_spark_session():
    """
    Get or create a Spark session

    Returns
    -------
    Spark session
       A Spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def get_song_df(spark, input_data):
    """
    Get the song data from the input path
    
    Parameters
    ----------
    spark : Spark session
        An active Spark seesionn
    input_data : string
        The path of the input data

    Returns
    -------
    Spark dataframe
        The song dataframe
    """
    # get filepath to song data file
    song_data_path = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    song_df = spark.read.json(song_data_path, schema=staging_song_schema)
    
    return song_df

def process_song_data(spark, input_data, output_data):
    """
    Process the song data into dimension tables
    
    Parameters
    ----------
    spark : Spark session
        An active Spark seesionn
    input_data : string
        The path of the input data
    output_data : string
        The path for the output
    """
    song_df = get_song_df(spark, input_data)
    song_df.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT 
            DISTINCT song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT 
            DISTINCT artist_id, 
            artist_name AS name, 
            artist_location AS location, 
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')

def process_log_data(spark, input_data, output_data):
    """
    Process the log data into fact and dimension tables
    
    Parameters
    ----------
    spark : Spark session
        An active Spark seesionn
    input_data : string
        The path of the input data
    output_data : string
        The path for the output
    """
    # get filepath to log data file
    log_data_path = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    log_df = spark.read.json(log_data_path)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')
    
    log_df.createOrReplaceTempView("staging_log")

    # extract columns for users table    
    artists_table = spark.sql(""" 
        SELECT 
            DISTINCT userId AS user_id, 
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
        FROM staging_log
        WHERE userId IS NOT NULL
    """)
    
    # write users table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x, TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1e3), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.ts))
    
    log_df.createOrReplaceTempView("staging_log")
    
    # extract columns to create time table
    time_table = spark.sql(""" 
        SELECT 
            start_time, 
            hour(start_time) as hour, 
            dayofmonth(start_time) as day, 
            weekofyear(start_time) as week, 
            month(start_time) as month, 
            year(start_time) as year, 
            dayofweek(start_time) as weekday
        FROM staging_log
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+'time/')

    # read in song data to use for songplays table
    song_df = get_song_df(spark, input_data)
    song_df.createOrReplaceTempView("staging_songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(""" 
        SELECT 
            monotonically_increasing_id() as songplay_id,
            l.start_time AS start_time,
            l.userId AS user_id,
            l.level AS level,
            s.song_id AS song_id,
            s.artist_id AS astist_id,
            l.sessionId AS session_id,
            l.location AS location,
            l.userAgent AS user_agent,
            year(l.start_time) as year,
            month(l.start_time) as month
        FROM  staging_log AS l
        LEFT JOIN staging_songs AS s
        ON l.artist = s.artist_name AND l.song = s.title
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+'songplays/')

def main():
    """
    Process the song and log data into fact and dimension tables
    """
    spark = create_spark_session()
    input_data = config['DATA']['INPUT']
    output_data = config['DATA']['OUTPUT']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
