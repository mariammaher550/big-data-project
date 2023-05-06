import pandas as pd
import numpy as np

# preprocessing books file
books_df = pd.read_csv("data/BX-Books.csv",
                       encoding="latin-1", low_memory=False)
books_df.book_author.fillna("unknown", inplace=True)
books_df.publisher.fillna("unknown", inplace=True)
books_df["year_of_publication"] = books_df["year_of_publication"].apply(
    lambda x: np.nan if not x.isdigit() else int(x))
year_mean = int(books_df.year_of_publication.mean())
books_df.year_of_publication.fillna(year_mean, inplace=True)
books_df.year_of_publication = books_df.year_of_publication.astype(int)
books_df.to_csv("data/books.csv", index=False)

# preprocessing users
users_df = pd.read_csv("data/BX-Users.csv",
                       encoding="latin-1", low_memory=False)
users_df.Location.fillna("unknown", inplace=True)
mean_age = users_df.Age.mean()
mean_age = int(mean_age)
users_df.Age.fillna(mean_age, inplace=True)
users_df.Age = users_df.Age.astype(int)
users_df.drop(users_df[users_df["user_id"] ==
              ', milan, italy"'].index, inplace=True)
users_df.to_csv("data/users.csv", index=False)

# preprocessing ratings
ratings_df = pd.read_csv("data/BX-Book-Ratings.csv",
                         encoding="latin-1", low_memory=False)
ratings_df.drop_duplicates(["isbn", "user_id"], inplace=True)
unique_user_ids = users_df['user_id'].unique()
ratings_filtered = ratings_df[ratings_df['user_id'].isin(unique_user_ids)]
unique_isbn = books_df['isbn'].unique()
ratings_filtered = ratings_df[ratings_df['isbn'].isin(unique_isbn)]
ratings_df.to_csv("data/ratings.csv", index=False)
