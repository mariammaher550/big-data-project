"""
This module provides a web-based dashboard for a book recommendation system.
"""
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import base64


def get_two_cols(data_path):
    """
        Reading the data and returning 2 columns of the data
    """
    col_1 = []
    col_2 = []
    with open(data_path, 'r') as file:
        for i, line in enumerate(file):
            if i == 0:
                continue
            line = line.strip()  # remove leading/trailing whitespace
            parts = line.split(',')  # split the line based on comma delimiter
            book_title = parts[0]  # extract the book title
            count = int(parts[-1])  # extract the count and convert to integer
            col_1.append(book_title)
            col_2.append(count)
    return col_1, col_2


def plot_bar(x, y, x_label, y_label, title):
    """
        Create a bar chart with age groups on the x-axis and number of users on the y-axis
    """
    fig, ax = plt.subplots()
    ax.bar(x, y, alpha=0.5, color='blue', edgecolor='black')
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.tick_params(axis='x', rotation=45)
    # Display the bar chart using Streamlit
    st.pyplot(fig, use_container_width=True)


def plot_table(data_path, col_name, title):
    """
        Plotting a table
    """
    age_range = []
    book = []
    count = []
    encountered_age_ranges = set()
    with open(data_path, 'r') as file:
        for i, line in enumerate(file):
            if i == 0:
                continue
            line = line.strip()  # remove leading/trailing whitespace
            parts = line.split(',')  # split the line based on comma delimiter
            curr_age_range = parts[0]
            if curr_age_range not in encountered_age_ranges:
                encountered_age_ranges.add(curr_age_range)
                age_range.append(curr_age_range)  # extract the age range
                book.append(' '.join(parts[1:-1]))  # extract the book title
                # extract the count and convert to integer
                count.append(int(parts[-1]))

    # Create a sample DataFrame
    df = pd.DataFrame({
        'Age Range': age_range,
        col_name: book,
    })

    st.write(title)
    # Display the DataFrame in a table format
    st.table(df)


def download_csv(data):
    """
    Function to download CSV file
    """
    csv = data.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="data.csv">Download CSV</a>'
    return href


st.write("# Big Data Project  \n Book Recommendation System  \n",
         "*Year*: **2023**")

ratings = pd.read_csv("../data/ratings.csv")
als_prediction = pd.read_csv('../output/als_predictions.csv')
dt_prediction = pd.read_csv('../output/dt_predictions.csv')
als_one_user = pd.read_csv('../output/als_rec_148744.csv')
dt_one_user = pd.read_csv('../output/dt_rec_148744.csv')

# Define main content
st.title('Data Characteristics')
st.write(f'Number of data instances: **{ratings.shape[0]}**')
st.write(f'Number of features: **{ratings.shape[1]}**')
st.write('### Feature Names')
features = ['user_id', 'isbn', 'location', 'age', 'book_title',
            'book_author', 'year_of_publication', 'publisher', 'rating']
for col in features:
    st.write(f'- {col}')


st.title('EDA')
# Load data
df_age_dist = pd.read_csv("../output/q1.csv")

# Define the age groups
bins = [0, 18, 25, 35, 55, 200]
labels = ['<18', '18-25', '26-35', '36-55', '55+']
df_age_dist['age_group'] = pd.cut(df_age_dist['age'], bins=bins, labels=labels)

# Group the dataframe by age_group and sum the number of users in each group
grouped_df = df_age_dist.groupby('age_group')['count'].sum().reset_index()

plot_bar(grouped_df['age_group'], grouped_df['count'],
         "Age Group", "Number of Users", 'Age Distribution of Users')


book_titles, counts = get_two_cols('../output/q2.csv')

plot_bar(book_titles[:5], counts[:5], 'Book Titles',
         'Counts', 'Book Counts by Title')


book_authors, authors_count = get_two_cols('../output/q3.csv')
plot_bar(book_authors[:5], authors_count[:5],
         'Book Author', 'Counts', 'Authors Popularity')

plot_table('../output/q4.csv', "Book Title",
           "Most Popular Title by Age Range")

plot_table('../output/q5.csv', "Book Author",
           "Most Popular Author by Age Range")

# Define RMSE values for two models
RMSE_MODEL1 = 5.32
RMSE_MODEL2 = 3.82

# Define section title
st.header('Models Performance')

# Define content
st.write('Here is the performance of the two models:')
st.write(f'- **ALS**: RMSE = {RMSE_MODEL1:.3f}')
st.write(f'- **Decision Trees**: RMSE = {RMSE_MODEL2:.3f}')

if st.checkbox("Show ALS predicitons"):
    st.write('## Data')
    st.write(als_prediction)
    st.markdown(download_csv(als_prediction), unsafe_allow_html=True)

if st.checkbox("Show DT predicitons"):
    st.write('## Data')
    st.write(dt_prediction)
    st.markdown(download_csv(dt_prediction), unsafe_allow_html=True)

# Define section title
st.header('Prediction for a single user')

if st.checkbox("ALS predictions for user_id = 148744"):
    st.write('## Data')
    st.write(als_one_user)
    st.markdown(download_csv(als_one_user), unsafe_allow_html=True)

if st.checkbox("DT predictions for user_id = 148744"):
    st.write('## Data')
    st.write(dt_one_user)
    st.markdown(download_csv(dt_one_user), unsafe_allow_html=True)
