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


spark = SparkSession.builder \
    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.1.0') \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()
input_path = os.getenv("INPUT_PATH")
output_path = os.getenv("OUTPUT_PATH")

# def read_data():
#     port_number = os.getenv("PSQL_PORTNUMBER")
#     db_name = os.getenv("PSQL_DBNAME")
#     username = os.getenv("PSQL_USERNAME")
#     password = os.getenv("PSQL_PASSWORD")
#     if not all([port_number, db_name, username, password]):
#         print("Missing one or more environment variables for database connection.")
#         return None
    

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


def write_data_mysql(df):
    return df



def main():
    df = read_data_cassandra()
    print("------SHOW DATA--------")
    df.show(5,truncate=False)
    print("------Print shemma--------")
    df.printSchema()
    print("------Transform data frame--------")
    spark.stop()


if __name__ == "__main__":
    main()