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
CREATE TABLE search (
        column1 int PRIMARY KEY,
        job_id int,
        benefits text,
        bid int,
        campaign_budget text,
        campaign_id int,
        city_name text,
        company_logo text,
        company_name text,
        description text,
        feed_id text,
        lat float,
        lon float,
        major_category text,
        minor_category text
);

CREATE TABLE search (
    column1 int PRIMARY KEY
    job_id int PRIMARY KEY,
    title TEXT,
    description TEXT,
    company_name TEXT,
    city_name TEXT,
    state TEXT,
    postal_code TEXT,
    campaign_id UUID,
    campaign_budget DOUBLE,
    pay_currency TEXT,
    pay_from DOUBLE,
    pay_to DOUBLE,
    pay_type TEXT,
    work_schedule TEXT,
    benefits LIST<TEXT>,
    bid DOUBLE,
    company_logo BLOB,
    feed_id UUID,
    lat DOUBLE,
    lon DOUBLE,
    major_category TEXT,
    minor_category TEXT,
    pay_option TEXT,
    requirements TEXT,
    status TEXT
);
CREATE TABLE search (
  job_id int,
  benefits text,
  campaign text,
  campaign_city text,
  company_name text,
  company_description text,
  feed_id int,
  latitude double,
  longitude double,
  major_category text,
  minor_category text,
  pay_currency text,
  pay_from int,
  pay_to int,
  pay_option text,
  pay_type text,
  postal_code text,
  required_skills list<text>,
  state text,
  status text,
  title text,
  work_schedule text,
  PRIMARY KEY (job_id)
);

select \* from search

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
