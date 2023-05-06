import pandas as pd
import numpy as np

# preprocessing books file
df = pd.read_csv("data/BX-Books.csv", encoding="latin-1", low_memory=False)
df.book_author.fillna("unknown", inplace=True)
df.publisher.fillna("unknown", inplace=True)
df["year_of_publication"] = df["year_of_publication"].apply(lambda x: np.nan if not x.isdigit() else int(x))
year_mean = int(df.year_of_publication.mean())
df.year_of_publication.fillna(year_mean, inplace=True)
df.year_of_publication = df.year_of_publication.astype(int)
df.to_csv("data/books.csv", index=False)

# preprocessing ratings
df = pd.read_csv("data/BX-Book-Ratings.csv", encoding="latin-1", low_memory=False)
df.drop_duplicates(["isbn", "user_id"], inplace=True)
df.to_csv("data/ratings.csv", index=False)

# preprocessing users
df = pd.read_csv("data/BX-Users.csv", encoding="latin-1", low_memory=False)
df.Location.fillna("unknown", inplace=True)
mean_age = df.Age.mean()
mean_age = int(mean_age)
df.Age.fillna(mean_age, inplace=True)
df.Age = df.Age.astype(int)
df.drop(df[df["user_id"] == ', milan, italy"'].index, inplace=True)
df.to_csv("data/users.csv", index=False)