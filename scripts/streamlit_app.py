import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd


def get_two_cols(data_path):
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
    # Create a bar chart with age groups on the x-axis and number of users on the y-axis
    fig, ax = plt.subplots()
    ax.bar(x, y, alpha=0.5, color='blue', edgecolor='black')
    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.tick_params(axis='x', rotation=45)
    # Display the bar chart using Streamlit
    st.pyplot(fig, use_container_width=True)
    st.pyplot(fig)


def plot_table(data_path, col_name, title):
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
                count.append(int(parts[-1]))  # extract the count and convert to integer

    # Create a sample DataFrame
    df = pd.DataFrame({
        'Age Range': age_range,
        col_name: book,
    })

    st.write(title)
    # Display the DataFrame in a table format
    st.table(df)


# Load data
df_age_dist = pd.read_csv("/project/output/q1.csv")

# Define the age groups
bins = [0, 18, 25, 35, 55, 200]
labels = ['<18', '18-25', '26-35', '36-55', '55+']
df_age_dist['age_group'] = pd.cut(df_age_dist['age'], bins=bins, labels=labels)

# Group the dataframe by age_group and sum the number of users in each group
grouped_df = df_age_dist.groupby('age_group')['count'].sum().reset_index()

plot_bar(grouped_df['age_group'], grouped_df['count'], "Age Group", "Number of Users", 'Age Distribution of Users')


book_titles, counts = get_two_cols('/project/output/q2.csv')

plot_bar(book_titles[:5], counts[:5], 'Book Titles', 'Counts', 'Book Counts by Title')


book_authors, authors_count = get_two_cols('/project/output/q3.csv')
plot_bar(book_authors[:5], authors_count[:5], 'Book Author', 'Counts', 'Authors Popularity')

plot_table('/project/output/q4.csv', "Book Title", "Most Popular Title by Age Range")

plot_table('/project/output/q5.csv', "Book Author", "Most Popular Author by Age Range")
