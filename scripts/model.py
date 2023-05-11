"""
In this module we train ALS and Decision Trees models for a book recommendation system.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor

spark = SparkSession.builder \
    .appName("BDT Project") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083") \
    .config("spark.sql.avro.compression.codec", "snappy") \
    .enableHiveSupport() \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')
print(spark.catalog.listDatabases())
print(spark.catalog.listTables("projectdb"))

# Read Hive table
print("Reading the hive tables")
users = spark.read.format("avro").table('projectdb.users')
books = spark.read.format("avro").table('projectdb.books')
book_ratings = spark.read.format("avro").table('projectdb.book_ratings')

users.createOrReplaceTempView('users')
books.createOrReplaceTempView('books')
book_ratings.createOrReplaceTempView('book_ratings')

# Preprocessing the data
print('Preprocessing the data')
book_ratings = book_ratings.withColumn(
    "clean_isbn", regexp_replace(col("isbn"), "(x|X)$", ""))
book_ratings = book_ratings.drop('isbn')
book_ratings = book_ratings.withColumn(
    "isbn", col("clean_isbn").cast(IntegerType()))
book_ratings = book_ratings.drop('clean_isbn')
book_ratings = book_ratings.na.drop(subset=["isbn"])

# Splitting the data
print('Spliting the data')
training, test = book_ratings.randomSplit([0.7, 0.3])
assembler = VectorAssembler(
    inputCols=["user_id", "isbn"], outputCol="features")
training = assembler.transform(training)
test = assembler.transform(test)

# Building the first model
print('Building the ALS model')
als = ALS(userCol="user_id", regParam=0.01, itemCol="isbn",
          ratingCol="rating", coldStartStrategy="drop")

# Cross validation and hyperparameter tuning the first model
pipeline = Pipeline(stages=[als])
paramGrid = ParamGridBuilder().addGrid(als.rank, [5, 10, 15]).addGrid(
    als.regParam, [0.001, 0.01, 0.2]).build()

crossval = CrossValidator(
    estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    ),
    numFolds=4
)

ALS_CV_MODEL = crossval.fit(training)

print('ALS predicting the test data')
als_prediction = ALS_CV_MODEL.transform(test)
als_prediction.show(10)
rmse = ALS_CV_MODEL.avgMetrics[0]
print("ALS RMSE = " + str(rmse))
# Saving first model's predictions
als_prediction.coalesce(1) \
    .select("prediction", 'rating') \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("sep", ",") \
    .option("header", "true") \
    .csv("output/als_predictions_dir")

# Prediction of a specific data sample
single_user = test.filter(test['user_id'] == 148744).select(
    ['user_id', 'isbn', 'rating'])
recomendations = ALS_CV_MODEL.transform(single_user)
recomendations = recomendations.orderBy('prediction', ascending=False)
recomendations.coalesce(1)\
    .select("prediction", 'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .csv("output/als_rec_148744_dir")

# Building the second model
print('Building the DT model')
dt = DecisionTreeRegressor(featuresCol="features", labelCol="rating")

# Cross validation and hyperparameter tuning the second model
pipeline = Pipeline(stages=[dt])
paramGrid = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [5, 10, 15]) \
    .addGrid(dt.minInstancesPerNode, [1, 5, 10]) \
    .build()

crossval = CrossValidator(
    estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    ),
    numFolds=4
)
DT_CV_MODEL = crossval.fit(training)
print("DT predicting test data")
dt_prediction = DT_CV_MODEL.transform(test)
rmse = DT_CV_MODEL.avgMetrics[0]
print("DT RMSE = " + str(rmse))
# Saving second model's predictions
dt_prediction.coalesce(1)\
    .select("prediction", 'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .csv("output/dt_predictions_dir")

# Prediction of a specific data sample
single_user = test.filter(test['user_id'] == 148744).select(
    ['features', 'rating'])
recomendations = DT_CV_MODEL.transform(single_user)
recomendations = recomendations.orderBy('prediction', ascending=False)
recomendations.coalesce(1)\
    .select("prediction", 'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header", "true")\
    .csv("output/dt_rec_148744_dir")
