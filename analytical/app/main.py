from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import count, col, spark_partition_id, lower, udf, broadcast, avg, rank, collect_list, current_timestamp, date_sub, year
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

#docker exec spark-spark-1 python3 main.py

# the spark session
spark = SparkSession.builder \
    .master("spark://spark:7077") \
    .config("spark.jars", "/app/gcs-connector-hadoop3-2.2.12.jar") \
    .config("spark.driver.extraClassPath", "/app/gcs-connector-hadoop3-2.2.12.jar") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# google cloud service account credentials file
spark._jsc.hadoopConfiguration().set(
    "google.cloud.auth.service.account.json.keyfile",
    "/app/credentials.json")

# bucket name
BUCKET_NAME = 'imdbbench'

# data frames
#category = spark.read.csv(f"gs://{BUCKET_NAME}/category.csv", header=True, inferSchema=True)
#genre = spark.read.csv(f"gs://{BUCKET_NAME}/genre.csv", header=True, inferSchema=True)
#name = spark.read.csv(f"gs://{BUCKET_NAME}/name.csv", header=True, inferSchema=True)
#title = spark.read.csv(f"gs://{BUCKET_NAME}/title.csv", header=True, inferSchema=True)
#titleAkas = spark.read.csv(f"gs://{BUCKET_NAME}/titleakas.csv", header=True, inferSchema=True)
#titleEpisode = spark.read.csv(f"gs://{BUCKET_NAME}/titleepisode.csv", header=True, inferSchema=True)
#titleGenre = spark.read.csv(f"gs://{BUCKET_NAME}/titlegenre.csv", header=True, inferSchema=True)
#titlePrincipals = spark.read.csv(f"gs://{BUCKET_NAME}/titleprincipals.csv", header=True, inferSchema=True)
#titlePrincipalsCharacters = spark.read.csv(f"gs://{BUCKET_NAME}/titleprincipalscharacters.csv", header=True, inferSchema=True)
#userHistory = spark.read.csv(f"gs://{BUCKET_NAME}/userhistory.csv", header=True, inferSchema=True)
#users = spark.read.csv(f"gs://{BUCKET_NAME}/users.csv", header=True, inferSchema=True)

#create parquet files
#category.write.parquet(f"gs://{BUCKET_NAME}/category.parquet")
#genre.write.parquet(f"gs://{BUCKET_NAME}/genre.parquet")
#name.write.parquet(f"gs://{BUCKET_NAME}/name.parquet")
#title.write.parquet(f"gs://{BUCKET_NAME}/title.parquet")
#titleAkas.write.parquet(f"gs://{BUCKET_NAME}/titleakas.parquet")
#titleEpisode.write.parquet(f"gs://{BUCKET_NAME}/titleepisode.parquet")
#titleGenre.write.parquet(f"gs://{BUCKET_NAME}/titlegenre.parquet")
#titlePrincipals.write.parquet(f"gs://{BUCKET_NAME}/titleprincipals.parquet")
#titlePrincipalsCharacters.write.parquet(f"gs://{BUCKET_NAME}/titleprincipalscharacters.parquet")
#userHistory.write.parquet(f"gs://{BUCKET_NAME}/userhistory.parquet")
#users.write.parquet(f"gs://{BUCKET_NAME}/users.parquet")

#read parquet files
category = spark.read.parquet(f"gs://{BUCKET_NAME}/category.parquet")
genre = spark.read.parquet(f"gs://{BUCKET_NAME}/genre.parquet")
name = spark.read.parquet(f"gs://{BUCKET_NAME}/name.parquet")
title = spark.read.parquet(f"gs://{BUCKET_NAME}/title.parquet")
titleAkas = spark.read.parquet(f"gs://{BUCKET_NAME}/titleakas.parquet")
titleEpisode = spark.read.parquet(f"gs://{BUCKET_NAME}/titleepisode.parquet")
titleGenre = spark.read.parquet(f"gs://{BUCKET_NAME}/titlegenre.parquet")
titlePrincipals = spark.read.parquet(f"gs://{BUCKET_NAME}/titleprincipals.parquet")
titlePrincipalsCharacters = spark.read.parquet(f"gs://{BUCKET_NAME}/titleprincipalscharacters.parquet")
userHistory = spark.read.parquet(f"gs://{BUCKET_NAME}/userhistory.parquet")
users = spark.read.parquet(f"gs://{BUCKET_NAME}/users.parquet")

#1.sql

#windowSpec = Window.partitionBy((title["start_year"] / 10) * 10).orderBy(title["id"], avg(userHistory["rating"]).desc())
#t_id = title \
#            .join(userHistory, userHistory["title_id"] == title["id"]) \
#            .where(title["title_type"] == "movie") \
#            .where(((title["start_year"] / 10) * 10) >= 1980) \
#            .join(titleGenre.join(genre, genre["id"] == titleGenre["genre_id"]) \
#                                        .where((genre["name"]).isin(["Drama"])) \
#                                        .select(titleGenre["title_id"]), title["id"] == titleGenre["title_id"], "inner") \
#            .join(titleAkas[titleAkas["region"].isin("US", "GB", "ES", "DE", "FR", "PT")] \
#                                        .select(titleAkas["title_id"]), title["id"] == titleAkas["title_id"], "inner") \
#            .groupBy(title["id"]) \
#            .agg(count(userHistory["rating"]).alias("rating_count")) \
#            .where("rating_count >= 3") \
#            .select(title["id"], \
#                    title["primary_title"], \
#                    ((title["start_year"] / 10) * 10).alias("decade"), \
#                    avg(userHistory["rating"]).alias("rating"), \
#                    (rank().over(windowSpec).alias("rank"))) \
#            .orderBy("decade", "rank".desc()) \
#            .collect()
#
#t_id \
#    .where("rank <= 10") \
#    .select("*") \
#    .show()

#2.sql
#title.alias("t") \
#    .join(titleEpisode.alias("te"), col("t.id") == col("te.parent_title_id")) \
#    .join(title.alias("t2"), col("t2.id") == col("te.title_id")) \
#    .join(userHistory.alias("uh"), col("uh.title_id") == col("t2.id")) \
#    .join(users.alias("u"), col("u.id") == col("uh.user_id")) \
#    .join((titleGenre.alias("tg").join(genre.alias("g"), col("g.id") == col("tg.genre_id")) \
#                    .select(col("tg.title_id"), col("g.name")) \
#                    .groupBy(col("tg.title_id")) \
#                    .agg(collect_list(col("g.name")).alias("genres"))).alias("tg"), \
#                col("t.id") == col("tg.title_id")) \
#    .where(title["title_type"] == "tvSeries") \
#    .where(userHistory["last_seen"].between(current_timestamp(), date_sub(current_timestamp(), 30))) \
#    .filter(titleEpisode["season_number"].isNotNull()) \
#    .filter(~users["country_code"].isin("US", "GB")) \
#    .select(title["id"], title["primary_title"], col("tg.genres"), titleEpisode["season_number"], count("*").alias("views")) \
#    .groupBy(title["id"], title["primary_title"], col("tg.genres"), titleEpisode["season_number"]) \
#    .orderBy(count("*").desc(), title["id"]) \
#    .limit(100) \
#    .show()
                    
#3.sql
name.alias("n") \
    .join(titlePrincipals.alias("tp"), col("tp.name_id") == col("n.id")) \
    .join(titlePrincipalsCharacters.alias("tpc"), (col("tpc.title_id") == col("tp.title_id")) & (col("tpc.name_id") == col("tp.name_id"))) \
    .join(category.alias("c"), col("c.id") == col("tp.category_id")) \
    .join(title.alias("t"), col("t.id") == col("tp.title_id")) \
    .join(titleEpisode.alias("te"), col("te.title_id") == col("tp.title_id"), "left") \
    .where(col("t.start_year") >= year(date_sub(current_timestamp(), 10))) \
    .where(col("c.name") == "actress") \
    .filter(col("n.death_year").isNull()) \
    .filter(col("t.title_type").isin("movie", "tvSeries", "tvMiniSeries", "tvMovie")) \
    .filter(col("te.title_id").isNull()) \
    .select(col("n.id"), col("n.primary_name"), year(date_sub(current_timestamp(), col("n.birth_year"))).alias("age")) \
    .groupBy(col("n.id")) \
    .agg(count("*").alias("roles")) \
    .orderBy(col("roles").desc()) \
    .limit(100) \
    .show()
    
                        

    




