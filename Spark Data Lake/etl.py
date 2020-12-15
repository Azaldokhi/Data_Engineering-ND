import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import databricks.koalas as ks
import pyspark.sql.functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function will loads song_data from S3 and processes songs table and artist table after that will load them                   back into S3 bucket
        
    Parameters:
            spark       = Spark Session
            input_data  = filepath of song_data 
            output_data = filepath of the output result 
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    kdf = ks.read_json(song_data)
    
    # extract columns to create songs table
    
    songs_table = (ks.sql('''
               SELECT 
               DISTINCT
               song_id,
               title,
               artist_id,
               year,
               duration
               FROM 
                   {kdf}''')
              )
    
    # convert Koalas datafream into Pyspark for indexing purpose 
    pyspark_songs_table = (songs_table
                        .to_spark()
                        .withColumn("id", F.monotonically_increasing_id())
                        )
    # convert it back to Koalas
    songs_table = ks.DataFrame(pyspark_songs_table)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.to_spark().write.mode('overwrite').partitionBy("year", "artist_id").parquet(os.path.join(output_data,'songs/'))
    
    # extract columns to create artists table

    artists_table = ks.sql('''
                SELECT DISTINCT artist_id, 
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
                FROM {kdf} 
                WHERE artist_id IS NOT NULL
 ''')
    
    # write artists table to parquet files
    artists_table.to_spark().write.mode('overwrite').parquet((os.path.join(output_data,'artists/'))

def process_log_data(spark, input_data, output_data):
    """
    Description: This function will loads log_data from S3 and processes users table and time table after that will load them                   back into S3 bucket
        
    Parameters:
            spark       = Spark Session
            input_data  = filepath of log_data 
            output_data = filepath of the output result 
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    kdf_log = ks.read_json(log_data)
    
    # filter by actions for song plays
    kdf_log = kdf_log.filter(kdf_log.page == 'NextSong')

    # extract columns for users table  
    
    users_table = ks.sql("""
                            SELECT DISTINCT userId as user_id, 
                            firstName as first_name,
                            lastName as last_name,
                            gender as gender,
                            level as level
                            FROM {df_log} 
                            WHERE userId IS NOT NULL
                        """)
    # write users table to parquet files
    users_table.to_spark().write.mode('overwrite').parquet((os.path.join(output_data,'users/'))

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    
    # extract columns to create time table
    time_df = ks.DataFrame(data = ks.to_datetime(kdf_log.head().ts, unit='ms'))
    
def extract_time_columns(kdf):
    """
    Description: This function will extract columns for time table
        
    Parameters:
            kdf = Koalas Dataframe
    """   
    kdf['hour'] = kdf.ts.dt.hour
    kdf['dayofweek'] = kdf.ts.dt.dayofweek
    kdf['year'] = kdf.ts.dt.year
    kdf['month'] = kdf.ts.dt.month
    return kdf

    time_df.pipe(extract_time_columns)
    
    # write time table to parquet files partitioned by year and month
    time_table.to_spark().write.mode("overwrite").partitionBy("year", "month").parquet((os.path.join(output_data,"time/"))

    # read in song data to use for songplays table
    #song_df = 
    
    #songplays_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ks.sql(""" 
                                select monotonically_increasing_id() as songplay_id, 
                                to_timestamp(logs.ts/1000) as start_time, 
                                month(to_timestamp(logs.ts/1000)) as month, 
                                year(to_timestamp(logs.ts/1000)) as year, 
                                logs.userId as user_id, logs.level as level, 
                                songs.song_id as song_id,
                                songs.artist_id as artist_id, 
                                logs.sessionId as session_id,
                                logs.location as location, 
                                logs.userAgent as user_agent 
                                FROM 
                                {df_log} logs JOIN {kdf} songs on logs.artist = songs.artist_name and logs.song = songs.title 
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table..to_spark().write.mode("overwrite").partitionBy("year", "month").parquet((os.path.join(output_data,"songplays/"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dokhi-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
