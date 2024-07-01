#Install libraries
import os
from dotenv import load_dotenv
import pandas as pd
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()
input_path = os.getenv("INPUT_PATH")
output_path = os.getenv("OUTPUT_PATH")


def read_data_cassandra():
    # Adjust keyspace and table names as per your Cassandra setup
    keyspace_name = "hhdatabase"
    table_name = "tracking"
    # Read data from Cassandra into a DataFrame
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace_name) \
        .load()
    return df

def transform_data(df):
    df = df.select('create_time','ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter((df.job_id.isNotNull()) & (df.custom_track.isNotNull()))
    df_filled = df.fillna({
        "bid": 0,
        "campaign_id": 0,
        "group_id": 0,
        "publisher_id": 0
    })
    return df_filled

def click_data(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.createOrReplaceTempView('tracking')
    clicks_output = spark.sql("""SELECT 
                                    DATE(ts) AS date,
                                    HOUR(ts) AS hour,
                                    job_id,
                                    publisher_id,
                                    campaign_id,
                                    group_id,
                                    COUNT(*) AS conversion
                                FROM tracking
                                GROUP BY DATE(ts), HOUR(ts), job_id, publisher_id, campaign_id, group_id""")
    return clicks_output


def write_data_mysql(df):
    port_number = os.getenv("MYSQL_PORTNUMBER")
    db_name = os.getenv("MYSQL_DBNAME")
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    
    return df



def main():
    df = read_data_cassandra()
    print("------Read data & show--------")
    df.show(5,truncate=False)
    print("------Print schema--------")
    df.printSchema()
    print("------Transform dataframe--------")
    df = transform_data(df)
    df.show(5,truncate=False)
    print("------Processing click data--------")
    # df = df.filter(df.custom_track == 'click')
    df = click_data(df)
    df.show(5,truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()