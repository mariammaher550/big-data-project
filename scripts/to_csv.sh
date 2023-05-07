echo "age,count" > /project/output/q1.csv
cat /project/output/q1/* >> /project/output/q1.csv

echo "book_title,rating_count" > /project/output/q2.csv
cat /project/output/q2/* >> /project/output/q2.csv

echo "book_author,rating_count" > /project/output/q3.csv
cat /project/output/q3/* >> /project/output/q3.csv

echo "age_range,book_title, rating_count" > /project/output/q4.csv
cat /project/output/q4/* >> /project/output/q4.csv

echo "age_range,book_author,rating_count" > /project/output/q5.csv
cat /project/output/q5/* >> /project/output/q5.csv