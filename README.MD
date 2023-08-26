# Book Recommendation System for Big Data Course

Welcome to the **Book Recommendation System** project repository, developed as part of the Big Data course. This project aims to build an end-to-end big data pipeline for recommending books based on user preferences and behavior build on [Kaggle dataset](https://www.kaggle.com/datasets/nabaanutthamaa/bookrecommendationsytsem).

## Project Structure

- **data/**: Contains dataset files.
- **models/**: Stores Spark ML models.
- **notebooks/**: Includes Jupyter or Zeppelin notebooks for learning purposes.
- **output/**: Represents the output directory for storing project results, including CSV files, text files, images, and other materials produced by the pipeline.
- **scripts/**: Houses .sh and .py scripts that make up the pipeline.
- **sql/**: Stores .sql and .hql files used in the project.
- **requirements.txt**: Lists the Python packages required to run project scripts. Feel free to add more packages as needed.
- **main.sh**: The main script that executes all pipeline stages, running the full pipeline and storing results in the output/ folder. Do not modify this script, as it will be used for assessment purposes.

## Project Pipeline

The project is divided into several stages:

### Data Collection and Ingestion
- A relational database is constructed using PostgreSQL.
- Relational data is imported into HDFS using Sqoop.

### Data Storage/Preparation
- Hive tables are created with a compressed file format (AVRO).
- Data is stored in the data warehouse Hive for analytics.

### Data Analysis
- Exploratory Data Analysis (EDA) is performed using HiveQL on the Tez engine.
- Predictive Data Analysis (PDA) is conducted by building machine learning models using SparkML.
- Feature extraction and data preprocessing are integral parts of this stage.

### Presentation
- Analysis results are presented in a user-friendly dashboard using Streamlit.

## Running the Project

To execute the project, upload the reposotory to HDB Sandbox and use the provided `main.sh` script. It will run all the pipeline scripts and store the results in the output/ folder.


