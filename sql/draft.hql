
DROP DATABASE IF EXISTS projectdb CASCADE;

CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

CREATE EXTERNAL TABLE users STORED AS AVRO LOCATION '/project/users' TBLPROPERTIES ('avro.schema.url'='/project/avsc/users.avsc');

CREATE EXTERNAL TABLE books STORED AS AVRO LOCATION '/project/books' TBLPROPERTIES ('avro.schema.url'='/project/avsc/books.avsc');

CREATE EXTERNAL TABLE book_ratings STORED AS AVRO LOCATION '/project/book_ratings' TBLPROPERTIES ('avro.schema.url'='/project/avsc/book_ratings.avsc');

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

CREATE EXTERNAL TABLE users(
    user_id int,
    location varchar(255),
    age int
    )
    PARTITIONED BY (location varchar(255)) STORED AS AVRO LOCATION '/project/users_part' TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');


