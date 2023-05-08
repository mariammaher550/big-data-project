from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import DecisionTreeRegressor

spark = SparkSession.builder\
        .appName("BDT Project")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris", "thrift://sandbox-hdp.hortonworks.com:9083")\
        .config("spark.sql.avro.compression.codec", "snappy")\
        .enableHiveSupport()\
        .getOrCreate()

print(spark.catalog.listDatabases())
print(spark.catalog.listTables("projectdb"))

# Read Hive table
users = spark.read.format("avro").table('projectdb.users')
books = spark.read.format("avro").table('projectdb.books')
book_ratings = spark.read.format("avro").table('projectdb.book_ratings')

users.createOrReplaceTempView('users')
books.createOrReplaceTempView('books')
book_ratings.createOrReplaceTempView('book_ratings')

# Preprocessing the data
book_ratings = book_ratings.withColumn("clean_isbn", regexp_replace(col("isbn"), "(x|X)$", ""))
book_ratings = book_ratings.drop('isbn')
book_ratings = book_ratings.withColumn("isbn", col("clean_isbn").cast(IntegerType()))
book_ratings = book_ratings.drop('clean_isbn')
book_ratings = book_ratings.na.drop(subset=["isbn"])

# Splitting the data
(training, test) = book_ratings.randomSplit([0.8, 0.2])
assembler = VectorAssembler(inputCols=["user_id", "isbn"], outputCol="features")
training = assembler.transform(training)
test = assembler.transform(test)

# Building the first model
als = ALS(userCol="user_id", regParam=0.01, itemCol="isbn", ratingCol="rating", coldStartStrategy="drop")

# Cross validation and hyperparameter tuning the first model
pipeline = Pipeline(stages=[als])
paramGrid = ParamGridBuilder().addGrid(als.maxIter, [5, 10]).build()

crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction"),numFolds=4)

als_cvModel = crossval.fit(training)
als_prediction = als_cvModel.transform(test)

# Saving first model's predictions
als_prediction.coalesce(1)\
    .select("prediction",'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .csv("/project/output/als_predictions.csv")

# Prediction of a specific data sample
single_user = test.filter(test['user_id']==148744).select(['user_id', 'isbn', 'rating'])
recomendations = als_cvModel.transform(single_user)
recomendations = recomendations.orderBy('prediction',ascending=False)
recomendations.coalesce(1)\
    .select("prediction",'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .csv("/project/output/als_rec_148744.csv")

# Building the second model
dt = DecisionTreeRegressor(featuresCol="features", labelCol="rating")

# Cross validation and hyperparameter tuning the second model
pipeline = Pipeline(stages=[dt])
paramGrid = ParamGridBuilder().addGrid(dt.maxDepth, [5, 10]).build()

crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid, evaluator=RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction"),numFolds=4)

dt_cvModel = crossval.fit(training)
dt_prediction = dt_cvModel.transform(test)

# Saving second model's predictions
dt_prediction.coalesce(1)\
    .select("prediction",'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .csv("/project/output/dt_predictions.csv")

# Prediction of a specific data sample
single_user = test.filter(test['user_id']==148744).select(['features', 'rating'])
recomendations = dt_cvModel.transform(single_user)
recomendations = recomendations.orderBy('prediction',ascending=False)
recomendations.coalesce(1)\
    .select("prediction",'rating')\
    .write\
    .mode("overwrite")\
    .format("csv")\
    .option("sep", ",")\
    .option("header","true")\
    .csv("/project/output/dt_rec_148744.csv")