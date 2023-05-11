echo "age,count" > output/q1.csv
cat output/q1/* >> output/q1.csv

echo "book_title,rating_count" > output/q2.csv
cat output/q2/* >> output/q2.csv

echo "book_author,rating_count" > output/q3.csv
cat output/q3/* >> output/q3.csv

echo "age_range,book_title, rating_count" > output/q4.csv
cat output/q4/* >> output/q4.csv

echo "age_range,book_author,rating_count" > output/q5.csv
cat output/q5/* >> output/q5.csv