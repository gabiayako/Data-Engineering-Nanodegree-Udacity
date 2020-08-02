import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS_CREDENTIALS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS_CREDENTIALS","AWS_SECRET_ACCESS_KEY")

def create_spark_session():
    """
    This function creates a spark session with pre defined configurations.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    - Reads data from a S3 bucket's song_data folder 
    
    - Selects some specific fields and assign it to songs_table
    
    - Writes a parquet file containing songs_table data partioned by year and artistId on a specific S3 bucket
    
    - Selects some specific fields and assign it to artists_table
    
    - Writes a parquet file containing artists_table data on a specific S3 bucket
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # song schema 
    song_log_schema = StructType([
        StructField("num_songs",IntegerType()),
        StructField("artist_id",StringType()),
        StructField("artist_latitude",DoubleType()),
        StructField("artist_longitude",DoubleType()),
        StructField("artist_location",StringType()),
        StructField("artist_name",StringType()),
        StructField("song_id",StringType()),
        StructField("title",StringType()),
        StructField("duration",DoubleType()),
        StructField("year",IntegerType()),
    ])

    # read song data file
    song_data = spark.read.json(song_data, schema=song_log_schema, mode="DROPMALFORMED")

    # extract columns to create songs table
    songs_table = song_data.select(
        ["song_id", "title", "artist_id", "year", "duration"]
    ).where(song_data["song_id"].isNotNull()).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = song_data.selectExpr([
        "artist_id",
        "artist_name as name",
        "artist_location as location",
        "artist_latitude as latitude",
        "artist_longitude as longitude"
    ]).where(song_data["artist_id"].isNotNull()).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")

    return songs_table

def process_log_data(spark, input_data, output_data, songs_table):
    """
    - Reads data from a S3 bucket's log_data folder 
    
    - Selects some specific fields and assign it to users_table
    
    - Writes a parquet file containing users_table data on a specific S3 bucket
    
    - Converts ts column to datetime
    
    - Selects some specific fields and assign it to time_table
    
    - Writes a parquet file containing time_table data partitioned by year and month on a specific S3 bucket
    
    - Joins data from log_data and songs_table and assing it to songplays_table
    
    - Writes a parquet file containing songplays_table data partitioned by year and month on a specific S3 bucket
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # event schema
    event_log_schema = StructType([
        StructField("artist",StringType()),
        StructField("auth",StringType()),
        StructField("firstName",StringType()),
        StructField("gender",StringType()),
        StructField("itemInSession",IntegerType()),
        StructField("lastName",StringType()),
        StructField("length",DoubleType()),
        StructField("level",StringType()),
        StructField("location",StringType()),
        StructField("method",StringType()),
        StructField("page",StringType()),
        StructField("registration",DoubleType()),
        StructField("sessionId",IntegerType()),
        StructField("song",StringType()),
        StructField("status",IntegerType()),
        StructField("ts",LongType()),
        StructField("userAgent",StringType()),
        StructField("userId",IntegerType()),
    ])

    # read log data file
    df = spark.read.json(log_data, schema=event_log_schema, mode="DROPMALFORMED")

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select([
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ]).where(
        df["userId"].isNotNull()
    ).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000)
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("datetime", get_datetime(df.timestamp))

    get_weekday = udf(lambda x: x.weekday(), IntegerType())
    df = df.withColumn("weekday", get_weekday(df.datetime))

    # extract columns to create time table
    time_table = df.select([
        "ts as start_time",
        "datetime.hour as hour",
        "datetime.day as day",
        "datetime.isocalendar()[1] as week",
        "datetime.month as month",
        "datetime.year as year",
        "weekday",
    ]).dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet", mode="overwrite")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songs_table.alias('s').join(
        df.alias('e'), col('s.title') == col('e.song')
    ).select([
        col('e.ts').alias('start_time'),
        col('e.userId').alias('user_id'),
        col('e.level').alias('level'),
        col('s.song_id').alias('song_id'),
        col('s.artist_id').alias('artist_id'),
        col('e.sessionId').alias('session_id'),
        col('e.location').alias('location'),
        col('e.userAgent').alias('user_agent'),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    ])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + "songplays_table.parquet", mode="overwrite")

def main():
    """
    - Creates a spark session

    - Assign values to input_data and output_data
    
    - Calls functions process_song_data and process_log_data which reads from S3 and loads to S3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-sparkify-project/"
    
    songs_table = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, songs_table)


if __name__ == "__main__":
    main()

