
DROP DATABASE IF EXISTS projectdb CASCADE;

CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

CREATE EXTERNAL TABLE users STORED AS AVRO LOCATION '/project/users' TBLPROPERTIES ('avro.schema.url'='/project/avsc/users.avsc');

CREATE EXTERNAL TABLE books STORED AS AVRO LOCATION '/project/books' TBLPROPERTIES ('avro.schema.url'='/project/avsc/books.avsc');

CREATE EXTERNAL TABLE book_ratings STORED AS AVRO LOCATION '/project/book_ratings' TBLPROPERTIES ('avro.schema.url'='/project/avsc/book_ratings.avsc');

CREATE TABLE users_partitioned (
  user_id INT,
  location VARCHAR(255),
  age INT
);

PARTITIONED BY (age_range BIGINT)
STORED AS AVRO
LOCATION '/project/users_partitioned'
TBLPROPERTIES ('avro.schema.url'='/project/avsc/users.avsc', 'avro.codec'='snappy');

INSERT OVERWRITE TABLE users_partitioned PARTITION (age_range)
SELECT user_id, location, age,
       CASE
           WHEN age BETWEEN 0 AND 20 THEN 1
           WHEN age BETWEEN 21 AND 30 THEN 2
           WHEN age BETWEEN 31 AND 40 THEN 3
           WHEN age BETWEEN 41 AND 50 THEN 4
           ELSE 5
       END AS age_range
FROM users;

ALTER TABLE users_partitioned RECOVER PARTITIONS;


