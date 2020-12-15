# Buiding DWH using AWS   

## Project Overview
Sparkify is a music streaming startup with a growing user base and song database.
Their user activity and songs metadata data resides in json files in S3. The goal of the project is to build an ETL pipeline that will extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


Our datasets are reside in S3. Here are the S3 links for each:

1. Song data: s3://udacity-dend/song_data
2. Log data: s3://udacity-dend/log_data
3. Log data json path: s3://udacity-dend/log_json_path.json


# Project Requirments 

## AWS 
1. AWS account
2. Amazon Redshift
3. Amazon S3 Bucket
4. IAM
5. EC2

## Python 
1. Pandas
2. psycopg2
3. Json
4. AWS SDK (boto3)
5. configparser

# DB Schema Design
### staging Table 
- staging_events
- staging_songs

### Fact Table 
- songplays 

### Dimensional Tables
- artists
- users
- time
- songs

# Execution Steps


1. Create AWS Account
2. Create IAM user with Administrator Access
3. Create a Redshift cluster.
4. Edit dwh.cfg file with the needed info 
4. Run create_tables.py in order to create the staging tables and the Star Schema Tables in and load them into Redshift DB cluster
5. Run etl.py to extract the data from S3 Bucket and transform the data and finally load them into the targeted tables .