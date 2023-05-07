
DROP DATABASE IF EXISTS projectdb CASCADE;

CREATE DATABASE projectdb;
USE projectdb;

SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;

CREATE EXTERNAL TABLE users STORED AS AVRO LOCATION '/project/users' TBLPROPERTIES ('avro.schema.url'='/project/avsc/users.avsc');

CREATE EXTERNAL TABLE books STORED AS AVRO LOCATION '/project/books' TBLPROPERTIES ('avro.schema.url'='/project/avsc/books.avsc');

CREATE EXTERNAL TABLE book_ratings STORED AS AVRO LOCATION '/project/book_ratings' TBLPROPERTIES ('avro.schema.url'='/project/avsc/book_ratings.avsc');

-- age distribution of users
INSERT OVERWRITE LOCAL DIRECTORY '/project/output/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT age, COUNT(*) as count
FROM users
GROUP BY age
ORDER BY age;

-- distribution of popularity by book title
INSERT OVERWRITE LOCAL DIRECTORY '/project/output/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT book_title, COUNT(*) as rating_count
FROM books JOIN book_ratings ON books.isbn = book_ratings.isbn
GROUP BY book_title
ORDER BY rating_count DESC;


-- distribution of popularity by book author
INSERT OVERWRITE LOCAL DIRECTORY '/project/output/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT book_author, COUNT(*) as rating_count
FROM books JOIN book_ratings ON books.isbn = book_ratings.isbn
GROUP BY book_author
ORDER BY rating_count DESC;

-- top 5 popular books by different age range
INSERT OVERWRITE LOCAL DIRECTORY '/project/output/q4'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT age_range, book_title, COUNT(*) as rating_count
FROM (
  SELECT
    CASE
      WHEN age < 18 THEN '<18'
      WHEN age BETWEEN 18 AND 25 THEN '18-25'
      WHEN age BETWEEN 26 AND 35 THEN '26-35'
      WHEN age BETWEEN 36 AND 55 THEN '36-55'
      ELSE '55+'
    END AS age_range,
    book_title,
    book_ratings.rating
  FROM users
  JOIN book_ratings ON users.user_id = book_ratings.user_id
  JOIN books ON book_ratings.isbn = books.isbn
) subquery
GROUP BY age_range, book_title
ORDER BY age_range, rating_count DESC;



-- top 5 popular authors by different age range
INSERT OVERWRITE LOCAL DIRECTORY '/project/output/q5'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT age_range, book_author, COUNT(*) as rating_count
FROM (
  SELECT
    CASE
      WHEN age < 18 THEN '<18'
      WHEN age BETWEEN 18 AND 25 THEN '18-25'
      WHEN age BETWEEN 26 AND 35 THEN '26-35'
      WHEN age BETWEEN 36 AND 55 THEN '36-55'
      ELSE '55+'
    END AS age_range,
    book_author,
    book_ratings.rating
  FROM users
  JOIN book_ratings ON users.user_id = book_ratings.user_id
  JOIN books ON book_ratings.isbn = books.isbn
) subquery
GROUP BY age_range, book_author
ORDER BY age_range, rating_count DESC;

