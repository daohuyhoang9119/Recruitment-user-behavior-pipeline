#Install libraries
import datetime
import os
import time
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

load_dotenv()


#----------Handle with database-----------------
def read_data_cassandra(mysql_time):
    # Adjust keyspace and table names as per your Cassandra setup
    keyspace_name = "hhdatabase"
    table_name = "tracking"
    # Read data from Cassandra into a DataFrame
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace_name) \
        .load().where(col('ts')>= mysql_time)
    return df

def read_company_from_mysql():
    # Retrieve environment variables
    load_dotenv()
    port_number = os.getenv("MYSQL_PORTNUMBER")
    db_name = os.getenv("MYSQL_DBNAME")
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    # Ensure all necessary environment variables are set
    if not all([db_name, username, password]):
        raise ValueError("Missing one or more required environment variables: MYSQL_DBNAME, MYSQL_USERNAME, MYSQL_PASSWORD")

    # Construct JDBC URL
    jdbc_url = f"jdbc:mysql://localhost:{port_number}/{db_name}"

    # Read data from MySQL table
    df = spark.read.format('jdbc').options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=sql,
        user=username,
        password=password
    ).load()
    return df




#----------Analyze data-----------------
def analyze_data(df):
    print("------Print schema--------")
    df.printSchema()
    print("------Custom track values--------")
    custom_track_counts = df.groupBy('custom_track').count()
    return custom_track_counts.show()


#----------TRANSFORM DATA-----------------

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

def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'click')
    conversion_data = conversion_data.na.fill({'bid':0})
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.createOrReplaceTempView('tracking')
    conversion_output = spark.sql("""SELECT 
                                    DATE(ts) AS date,
                                    HOUR(ts) AS hour,
                                    job_id,
                                    publisher_id,
                                    campaign_id,
                                    group_id,
                                    COUNT(*) AS conversion
                                FROM tracking
                                GROUP BY DATE(ts), HOUR(ts), job_id, publisher_id, campaign_id, group_id""")
    return conversion_output

def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.registerTempTable('clicks')
    clicks_output = spark.sql("""SELECT job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , avg(bid) as bid_set, count(*) as clicks,  sum(bid) as spend_hour   from clicks
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return clicks_output 

def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'bid':0})
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.registerTempTable('unqualified')
    unqualified_output = spark.sql("""SELECT job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as unqualified from unqualified 
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return unqualified_output 

def calculating_qualified(df):
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'bid':0})
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.registerTempTable('qualified')
    qualified_output = spark.sql("""SELECT job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id, count(*) as qualified  
                                from qualified
                                group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return qualified_output 

def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
                                join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full').\
                                join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full')
    return final_data 
    
def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data



def write_data_mysql(df):
    port_number = os.getenv("MYSQL_PORTNUMBER")
    db_name = os.getenv("MYSQL_DBNAME")
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    table_name = 'events'
    # Ensure all necessary environment variables are set
    if not all([db_name, username, password]):
        raise ValueError("Missing one or more required environment variables: MYSQL_DBNAME, MYSQL_USERNAME, MYSQL_PASSWORD")
    jdbc_url = f"jdbc:mysql://localhost:{port_number}/{db_name}"
    
    # final_output = df.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversions','clicks','bid_set','spend_hour')
    df = df.withColumnRenamed('date','dates')\
                                .withColumnRenamed('hour','hours')\
                                .withColumnRenamed('qualified','qualified_application').\
                                withColumnRenamed('unqualified','disqualified_application')\
                                .withColumnRenamed('conversions','conversion')
    df = df.withColumn('sources',lit('Cassandra'))
   
    df.write.format('jdbc').options(
        url=jdbc_url,
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=table_name,
        user=username,
        password=password
    ).mode('append').save()
    return print('Data imported successfully')




def main(mysql_time):
    df = read_data_cassandra(mysql_time)
    print("------Read data & show--------")
    # df.show(5,truncate=False)
    last_updated_time = df.select(f.max('ts')).collect()[0][0]

    print("------Transform data--------")
    df = transform_data(df)
    # df.show(5,truncate=False)

    print("------Processing and calculating form Cassandra--------")
    df = process_cassandra_data(df)
    # df.show(5,truncate=False)

    print("------Processing company table from MySQL--------")
    company_df = read_company_from_mysql()
    # company_df.show(5,truncate=False)


    print("------Join together--------")
    final_output = df.join(company_df,'job_id','left').drop(company_df.group_id).drop(company_df.campaign_id)
    final_output = final_output.withColumn('last_updated_at',f.lit(last_updated_time))

    print("------Import result to MySQL--------")
    write_data_mysql(final_output)
    
    print("------Done Job--------")
    spark.stop()

# if __name__ == "__main__":
#     main()

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'hhdatabase').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_latest_time_mysql(url,driver,user,password):    
    sql = """(select max(last_updated_at) from events) data"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        # mysql_latest = '2023-01-14 21:56:05'
        mysql_latest = '2023-01-14 22:56:05'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest 

db_name = os.getenv("MYSQL_DBNAME")
user = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
url = os.getenv("MYSQL_DRIVER")
driver = "com.mysql.cj.jdbc.Driver"


#run the while loop to compare data old and new
while True :
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_latest_time_mysql(url,driver,user,password)
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        main(mysql_time)
    else :
        print("No new data found")
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(10)
    