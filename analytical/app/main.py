from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import count, spark_partition_id, lower, udf, broadcast
from pyspark.sql.types import StringType

#docker exec spark-spark-1 python3 main.py

# the spark session
spark = SparkSession.builder \
    .master("spark://spark:7077") \
    .config("spark.jars", "/app/gcs-connector-hadoop3-2.2.12.jar") \
    .config("spark.driver.extraClassPath", "/app/gcs-connector-hadoop3-2.2.12.jar") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

# google cloud service account credentials file
spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.json.keyfile",
    "/app/credentials.json")

# bucket name
BUCKET_NAME = 'imdbbench'

# data frames
#names = spark.read.csv(f"gs://{BUCKET_NAME}/name.basics.tsv")
#akas = spark.read.csv(f"gs://{BUCKET_NAME}/title.akas.tsv")
#basics = spark.read.csv(f"gs://{BUCKET_NAME}/title.basics.tsv")
#principals = spark.read.csv(f"gs://{BUCKET_NAME}/title.principals.tsv")
#crew = spark.read.csv(f"gs://{BUCKET_NAME}/title.crew.tsv")
#episode = spark.read.csv(f"gs://{BUCKET_NAME}/title.episode.tsv")

#create a view to use with sql
#names.createOrReplaceTempView("names")
#akas.createOrReplaceTempView("akas")
#basics.createOrReplaceTempView("basics")
#principals.createOrReplaceTempView("principals")
#crew.createOrReplaceTempView("crew")
#episode.createOrReplaceTempView("episode")

#convert all dataframes to parquet
#names.write.parquet(f"gs://{BUCKET_NAME}/name.basics.parquet")
#akas.write.parquet(f"gs://{BUCKET_NAME}/title.akas.parquet")
#basics.write.parquet(f"gs://{BUCKET_NAME}/title.basics.parquet")
#principals.write.parquet(f"gs://{BUCKET_NAME}/title.principals.parquet")
#crew.write.parquet(f"gs://{BUCKET_NAME}/title.crew.parquet")
#episode.write.parquet(f"gs://{BUCKET_NAME}/title.episode.parquet")

#read parquet files
names = spark.read.parquet(f"gs://{BUCKET_NAME}/name.basics.parquet")
akas = spark.read.parquet(f"gs://{BUCKET_NAME}/title.akas.parquet")
basics = spark.read.parquet(f"gs://{BUCKET_NAME}/title.basics.parquet")
principals = spark.read.parquet(f"gs://{BUCKET_NAME}/title.principals.parquet")
crew = spark.read.parquet(f"gs://{BUCKET_NAME}/title.crew.parquet")
episode = spark.read.parquet(f"gs://{BUCKET_NAME}/title.episode.parquet")

#get the first line of each dataframe
names \
    .select("*") \
    .limit(10) \
    .show(10, False)
akas \
    .select("*") \
    .limit(10) \
    .show(10, False)
basics \
    .select("*") \
    .limit(10) \
    .show(10, False)
principals \
    .select("*") \
    .limit(10) \
    .show(10, False)
crew \
    .select("*") \
    .limit(10) \
    .show(10, False)
episode \
    .select("*") \
    .limit(10) \
    .show(10, False)




