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


spark = SparkSession.builder.config("spark.driver.memory", "10g").getOrCreate()
input_path = os.getenv("INPUT_PATH")
output_path = os.getenv("OUTPUT_PATH")

def read_data():
    port_number = os.getenv("PSQL_PORTNUMBER")
    db_name = os.getenv("PSQL_DBNAME")
    username = os.getenv("PSQL_USERNAME")
    password = os.getenv("PSQL_PASSWORD")
    if not all([port_number, db_name, username, password]):
        print("Missing one or more environment variables for database connection.")
        return None



def main(path):
    return 1


if __name__ == "__main__":
    main()