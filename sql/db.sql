
\c project;

CREATE TABLE users (
  user_id INTEGER NOT NULL PRIMARY KEY,
  location VARCHAR(255) NOT NULL,
  age INTEGER NOT NULL
);

CREATE TABLE books (
  isbn VARCHAR(255) NOT NULL PRIMARY KEY,
  book_title VARCHAR(512) NOT NULL,
  book_author VARCHAR(225) NOT NULL,
  year_of_publication INTEGER NOT NULL,
  publisher VARCHAR(225) NOT NULL

);

CREATE TABLE book_ratings (
  user_id INTEGER NOT NULL,
  isbn VARCHAR(255) NOT NULL,
  rating INTEGER NOT NULL,
  PRIMARY KEY (user_id, isbn)
);

--
ALTER TABLE book_ratings ADD CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users (user_id);
ALTER TABLE book_ratings ADD CONSTRAINT fk_isbn FOREIGN KEY (isbn) REFERENCES books(isbn);

--
\COPY books (isbn, book_title, book_author, year_of_publication, publisher) FROM 'data/books.csv' DELIMITER ',' CSV HEADER;
\COPY users (user_id, location, age) FROM 'data/users.csv' DELIMITER ',' CSV HEADER;
\COPY book_ratings (user_id, isbn, rating) FROM 'data/ratings.csv' DELIMITER ',' CSV HEADER;
