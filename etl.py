#Importing the essential libraries
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

#Seting the Parser Configuration
config = configparser.ConfigParser()
config.read('dl.cfg')

#Setting the OS environment
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    #Setting the environment with Spark using Spark Session Library
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song_data'
    input_data = "s3a://udacity-dend/"
    #song_data = os.path.join(input_data, "song_data/A/A/A/TRAAAAK128F9318786.json")

    
    # read song data file
    df = spark.read.json("data/local-Songdata/song_data/A/A/A/TRAAAAW128F429D538.json")

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title','artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet('s3a://myawss3bucket22/song_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name'.alias('name'), \

    'artist_location'.alias('location'), \

    'artist_latitude'.alias('lattitude'), \

    'artist_longitude'.alias('longitude')) 
    
    # write artists table to parquet files
    artists_table.write.parquet('s3a://myawss3bucket22/artist_table')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data" 

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').select('artist', 'auth', 'firstName', \
    'gender', 'iteminSession', 'lastName', \
    'length','level', 'location','method', \
    'page', 'registration', 'sessionId',\
    'song', 'status', 'ts', 'userAgent', 'userId' )

    # extract columns for users table    
    # artists_table =
    users_table = df.select('userid', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    # artists_table
    users_table.write.parquet('s3a://myawss3bucket22/usersTable')

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = df.withColumn("time_stamp", F.to_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = df.withColumn("date_time", date_format("ts"))
    
    # extract columns to create time table
    time_table = df.select('time_stamp', hour("ts").alias('hour'), \
    date_format('ts', 'F').alias('day'), \
    weekofyear("ts").alias('week'), \
    month("ts").alias('month'), \
    year("ts").alias('year'), date_format('ts', 'E').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    # using the correct setting of the s3 address
    time_table.write.parquet('s3a://myawss3bucket22/timeTable')

    # read in song data to use for songplays table
    song_df = songs_table 

    # extract columns from joined song and log datasets to create songplays table 
    #songplays_table = 
    songdf.createOrReplaceTempView("song_table")
    df.createOrReplaceTempView("log_table")
    songplays_table_df = spark.select("select s.song_id as songplay_id, \
    l.start_time, l.userId, l.level, s.song_id, \
    s.artist_id, l.sessionid, l.location, l.userAgent \
    from song_table s join \
    log_table l \
    on l.artist = s.artist_name and l.song = s.title where l.page = 'NextSong' ")
    songplays_table = songplays_table_df.toPandas()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet('s3a://myawss3bucket22/songPlays_table')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/song_data/output_data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
