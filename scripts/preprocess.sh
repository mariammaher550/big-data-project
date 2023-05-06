#!/bin/bash

echo "installing requirements"
pip install -r requirements.txt --quiet
python scripts/preprocessing.py
#
#echo "preprocessing first file"
#echo 'import pandas as pd
#df = pd.read_csv("data/BX-Users.csv", encoding="latin-1", low_memory=False)
#df.Location.fillna("unknown", inplace=True)
#mean_age = df.Age.mean()
#mean_age = int(mean_age)
#df.Age.fillna(mean_age, inplace=True)
#df.to_csv("data/users.csv", index=False)' > prep_users.py
#python  prep_users.py
#
#echo "preprocessing second file"
#echo 'import pandas as pd
#df = pd.read_csv("data/BX-Books.csv", encoding="latin-1", low_memory=False)
#df.book_author.fillna("unknown", inplace=True)
#df.publisher.fillna("unknown", inplace=True)
#df.to_csv("data/books.csv", index=False)
#' > prep_books.py
#python prep_books.py
#
#echo "preprocessing third file"
#echo 'import pandas as pd
#df = pd.read_csv("data/BX-Book-Ratings.csv", encoding="latin-1", low_memory=False)
#df.drop_duplicates(inplace=True)
#df.to_csv("data/ratings.csv", index=False)' > prep_ratings.py
#python prep_ratings.py