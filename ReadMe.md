# Recruitment User Behavior Pipeline

## Overview

This project builds a real-time/near-real-time ETL pipeline that extracts data from Cassandra (DL), transforms it using PySpark, and loads it into MySQL (DW). The core functionality involves connecting to Cassandra to read data, processing it into the desired format, and importing the processed data into MySQL.

## Project Structure

The project is organized into the following main components:

```bash
   recruitment-user-behavior-pipeline/

    ├── job/
    │   ├── main.py
    |── requirements.txt
    ├── .env.example
    ├── .gitignore
    ├── data-dictionary.xlsx
    ├── README.md
```

- **main.py**: Contains the main script to orchestrate the data pipeline.
- **job/**: Directory containing scripts and modules related to data processing.
- **data/**: Directory where input Parquet files are stored.
- **output/**: Directory where output files (such as CSV files) are stored.

## Data Schemma

Cassandra

```bash
|-- eventID: string (nullable = true)
|-- datetime: string (nullable = true)
|-- user_id: string (nullable = true)
|-- keyword: string (nullable = true)
|-- category: string (nullable = true)
|-- proxy_isp: string (nullable = true)
|-- platform: string (nullable = true)
|-- networkType: string (nullable = true)
|-- action: string (nullable = true)
|-- userPlansMap: array (nullable = true)
|    |-- element: string (containsNull = true)
```

MySQL

```bash
|-- eventID: string (nullable = true)
|-- datetime: string (nullable = true)
|-- user_id: string (nullable = true)
|-- keyword: string (nullable = true)
|-- category: string (nullable = true)
|-- proxy_isp: string (nullable = true)
|-- platform: string (nullable = true)
|-- networkType: string (nullable = true)
|-- action: string (nullable = true)
|-- userPlansMap: array (nullable = true)
|    |-- element: string (containsNull = true)
```

Output

```bash
|-- user_id: string (nullable = true)
|-- most_search_t6: string (nullable = true)
|-- most_search_t7: string (nullable = true)

```

## Data Processing Pipeline

- Read Data from Cassandra:
  Read data from the tracking table.
- Read Company Data from MySQL:
  Read and transform the job table from MySQL.
- Transform Data:
  Selects relevant columns and fills missing values.
- Calculate Metrics:
  Calculates clicks, conversions, qualified, and unqualified events.
- Join Data:
  Joins the transformed Cassandra data with MySQL data on job_id.
- Write Data to MySQL:
  Writes the final joined and transformed data back to MySQL.

## Requirements

To run this project, ensure you have the following dependencies installed:

- Python (>= 3.x)
- Apache Spark
- PySpark
- pandas
- findspark
- dotenv
- Docker
- Cassandra
- MySQL
- DataGrip

## Models

- Linear regression

## Tech Stack

**Client:** React, Redux, TailwindCSS

**Server:** Node, Express

Requirements : Build a realtime / near realtime ETL pipeline from DL to DW using PySpark
DL : Cassandra
DW : MySQL

Core : ETL Pipeline from DL to DW using PySpark (1)
Add ons : Real time / near realtime (2)

(1) 1. Extract : Đọc được data từ DL (Dùng pyspark connect tới Cassandra và đọc data) 2. Transform : Xử lý thành output mong muốn 3. Load : import dữ liệu sau khi xử lý xuống DW (Dùng pyspark import data xuống MySQL)

(2) Thêm tính năng realtime / near realtime

Docker start Mysql
docker run -d --name my-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=1 mysql

---

Docker start Cassandra
docker run -d --name my-cassandra -p 9042:9042 -e CASSANDRA_CLUSTER_NAME=gotoro -e CASSANDRA_USER=cassandra -e CASSANDRA_PASSWORD=cassandra cassandra
----- Code cassandra-----
b1: cai jdbc cassandra cho dbeaver
b2: vao terminal docker cho cassandra

- describe keyspaces => chon keyspaces, vd: hhdatabase
  b3: vao dbeaver
- create a connection for cassandra =>
  jdbc url: jdbc:cassandra://localhost:9042/{ mykeyspace }?localdatacenter=datacenter1
  doi keyspace, o day la jdbc:cassandra://localhost:9042/hhdatabase?localdatacenter=datacenter1

b3: create table

```
C:\Users\DELL\DataGripProjects\


use hhdatabase;
CREATE TABLE hhdatabase.tracking (
    create_time  text  PRIMARY KEY,
    bid          double,
    bn           text,
    campaign_id  double,
    cd           double,
    custom_track text,
    de           text,
    dl           text,
    dt           text,
    ed           text,
    ev           double,
    group_id     double,
    id           text ,
    job_id       double,
    md           text,
    publisher_id double,
    rl           text,
    sr           text,
    ts           text,
    tz           double,
    ua           text,
    uid          text,
    utm_campaign text,
    utm_content  text,
    utm_medium   text,
    utm_source   text,
    utm_term     text,
    v            double,
    vp           text
);


drop table hhdatabase.tracking;

select * from tracking limit 10;




```

Docker file compose

FROM apache/spark-py:v3.1.3

USER root
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
RUN apt-get update
RUN apt install -y python3 python3-pip
RUN pip3 install --upgrade pip setuptools --user
RUN rm -r /root/.cache && rm -rf /var/cache/apt/\*

WORKDIR /opt/application
COPY requirements.txt .
COPY pyspark_etl_auto.py /opt/application/pyspark_etl_auto.py
COPY mysql-connector-java-8.0.30.jar /opt/spark/jars
COPY entrypoint.sh /entrypoint.sh

RUN pip3 install -r requirements.txt --user

ENTRYPOINT ["sh","/entrypoint.sh"]
