"""
This module provides a web-based dashboard for a book recommendation system.
"""
import base64
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
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



def plot_bar(data, x_label, y_label, title, description):
    """
    Create a bar chart with age groups on the x-axis and number of users on the y-axis
    """
    x_data, y_data = data
    st.write(description)
    fig, axis = plt.subplots()
    axis.bar(x_data, y_data, alpha=0.5, color='blue', edgecolor='black')
    axis.set_title(title)
    axis.set_xlabel(x_label)
    axis.set_ylabel(y_label)
    axis.tick_params(axis='x', rotation=45)
    # Display the bar chart using Streamlit
    st.pyplot(fig, use_container_width=True)


def plot_table(data_path, col_name, title, description):
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
    data = pd.DataFrame({
        'Age Range': age_range,
        col_name: book,
        'Count': count
    })

    st.write(title)
    st.write(description)
    # Display the DataFrame in a table format
    st.table(data)




def download_csv(data):
    """
    Function to download CSV file
    """
    csv = data.to_csv(index=False)
    b64 = base64.b64encode(csv)
    href = '<a href="data:file/csv;base64,{0}" download="data.csv">Download CSV</a>'.format(b64)
    return href


st.write("# Big Data Project")
st.write("Book Recommendation System")
st.write("Year: 2023")

ratings = pd.read_csv("ratings.csv")
als_prediction = pd.read_csv('/project/output/als_predictions.csv/als_predictions.csv')
dt_prediction = pd.read_csv('/project/output/als_predictions.csv/dt_predictions.csv')
als_one_user = pd.read_csv('/project/output/als_predictions.csv/als_rec_148744.csv')
dt_one_user = pd.read_csv('/project/output/als_predictions.csv/dt_rec_148744.csv')

# Define main content
st.title('Data Characteristics')
st.write('Number of data instances: **' + str(ratings.shape[0]) + '**')
features = ['user_id', 'isbn', 'location', 'age', 'book_title',
            'book_author', 'year_of_publication', 'publisher', 'rating']
st.write('Number of features: **' + str(len(features)) + '**')
st.write('### Feature Names')

for col in features:
    st.write('- ' + col)

st.title('EDA')
# Load data
df_age_dist = pd.read_csv("/project/output/q1.csv")

# Define the age groups
bins = [0, 18, 25, 35, 55, 200]
labels = ['<18', '18-25', '26-35', '36-55', '55+']
df_age_dist['age_group'] = pd.cut(df_age_dist['age'], bins=bins, labels=labels)

# Group the dataframe by age_group and sum the number of users in each group
grouped_df = df_age_dist.groupby('age_group')['count'].sum().reset_index()

plot_bar((grouped_df['age_group'], grouped_df['count']),
         "Age Group", "Number of Users", 'Age Distribution of Users',
         description="Here we could see that users "
         "age distribution is a normal distribution."
         " In which most users' age is between 26-35.")

book_titles, counts = get_two_cols('/project/output/q2.csv')

plot_bar((book_titles[:5], counts[:5]), 'Book Titles',
         'Counts', 'Book Counts by Title',
         description="Here we can see the 5 top most rated books by all users. "
         "Wild Animus comes first followed by the Lovely Bones and Davinci Code.")

book_authors, authors_count = \
    get_two_cols('/project/output/q3.csv')
plot_bar((book_authors[:5], authors_count[:5]),
         'Book Author', 'Counts', 'Authors Popularity',
         description="Here we can see the 5 top most rated "
         "authors by all users. Stephan king comes first followed by Nora Roberts and John Gresham."
         )

plot_table('/project/output/q4.csv', "Book Title",
           "Most Popular Title by Age Range",
           description="Here we could see most popular book in each age range."
           " Wild Animus dominates."
           )

plot_table('/project/output/q5.csv', "Book Author",
           "Most Popular Author by Age Range",
           description="Here we could see most popular authors in each age range. "
                       "In which Stephan King is most popular among young adults and middle age,"
                       " while Nora Roberts appeals more to an older audience, "
                       "And R.L Stine appeals more to younger audience ")

# Define RMSE values for two models
RMSE_MODEL1 = 5.32
RMSE_MODEL2 = 3.82

# Define section title
st.header('Models Performance')

# Define content
st.write('Here is the performance of the two models:')
st.write('- **ALS**: RMSE = ' + str('%.3f' % RMSE_MODEL1))
st.write('- **Decision Trees**: RMSE = ' + str('%.3f' % RMSE_MODEL2))


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
