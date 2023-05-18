import time
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.functions import count, col, spark_partition_id, lower, udf, broadcast, avg, rank, collect_list, current_timestamp, date_sub, year, substring, floor
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType

#docker exec spark-spark-1 python3 main.py

# the spark session
spark = SparkSession.builder \
    .master("spark://spark:7077") \
    .config("spark.jars", "/app/gcs-connector-hadoop3-2.2.12.jar") \
    .config("spark.driver.extraClassPath", "/app/gcs-connector-hadoop3-2.2.12.jar") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.executor.memory", "1g") \
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

start = time.time()

rank_window = Window.partitionBy(col("decade")) \
    .orderBy(col("rating_avg").desc(), title["id"])

filteredTitle = title \
    .filter((title["title_type"] == "movie") & ((floor(title["start_year"] / 10) * 10).cast("int") >= 1980))

query1 = filteredTitle \
    .join(titleAkas[titleAkas["region"].isin("US", "GB", "ES", "DE", "FR", "PT")]
          .select(titleAkas["title_id"]), filteredTitle["id"] == titleAkas["title_id"], "inner") \
    .join(titleGenre.join(genre, genre["id"] == titleGenre["genre_id"])
          .where((genre["name"]).isin(["Drama"]))
          .select(titleGenre["title_id"]), filteredTitle["id"] == titleGenre["title_id"], "inner") \
    .join(userHistory, userHistory["title_id"] == filteredTitle["id"]) \
    .groupBy(filteredTitle["id"], filteredTitle["primary_title"], (floor(filteredTitle["start_year"] / 10) * 10).cast("int")) \
    .agg(filteredTitle["id"],
         filteredTitle["primary_title"],
         (floor(filteredTitle["start_year"] / 10)
          * 10).cast("int").alias("decade"),
         avg(userHistory["rating"].cast("double")).alias("rating_avg"),
         count(userHistory["rating"]).alias("rating_count")
         ) \
    .where(col("rating_count") >= 3) \
    .withColumn("rank", rank().over(rank_window)) \
    .select(filteredTitle["id"],
            filteredTitle["primary_title"].substr(1, 30).alias("left"),
            col("decade"),
            col("rating_avg"),
            col("rank")) \
    .where(col("rank") <= 10)\
    .orderBy(col("decade"), col("rating_avg").desc())

query1.show(100, False)
end = time.time()
print("1.sql time: ", end - start)
query1.printSchema()
query1.select("rating_avg").distinct().show()

#2.sql
#start = time.time()
#title_filtered = title \
#        .where(title["title_type"] == "tvSeries") \
#        .select(title["id"], title["primary_title"]) 
#
#userHistory_filtered = userHistory \
#        .where(userHistory["last_seen"].between(date_sub(current_timestamp(), 30), current_timestamp())) \
#        .select(userHistory["user_id"], userHistory["title_id"])
#
#titleEpisode_filtered = titleEpisode \
#        .filter(titleEpisode["season_number"].isNotNull()) \
#        .select(titleEpisode["title_id"], titleEpisode["parent_title_id"],titleEpisode["season_number"])
#
#users_filtered = users \
#        .filter(~users["country_code"].isin("US", "GB")) \
#        .select(users["id"]) 
#
#genre_filtered = genre \
#        .filter(genre["name"].isin("Drama")) \
#        .select(genre["id"], genre["name"])
#
#titleGenre_filtered = titleGenre \
#        .join(genre_filtered, genre_filtered["id"] == titleGenre["genre_id"]) \
#        .groupBy(titleGenre["title_id"]) \
#        .agg(collect_list(genre_filtered["name"]).alias("genres")) \
#        .select(titleGenre["title_id"], col("genres"))
#
#users_filtered.alias("u") \
#        .join(userHistory_filtered.alias("uh"), col("u.id") == col("uh.user_id")) \
#        .join(title.alias("t2"), col("uh.title_id") == col("t2.id")) \
#        .join(titleEpisode_filtered.alias("te"), col("te.title_id") == col("t2.id")) \
#        .join(title_filtered.alias("t"), col("te.parent_title_id") == col("t.id")) \
#        .join(titleGenre_filtered.alias("tg"), col("tg.title_id") == col("t.id")) \
#        .groupBy(col("t.id"), col("t.primary_title"), col("tg.genres"), col("te.season_number")) \
#        .agg(count("*").alias("views")) \
#        .orderBy(col("views").desc(), "t.id") \
#        .show()
#       
#end = time.time()
#print("2.sql time: ", end - start)
#                    
##3.sql
#start = time.time()
#ten_years = 365 * 10
#
#title_filtered = title \
#        .where(title["start_year"] >= year(date_sub(current_timestamp(), ten_years))) \
#        .filter(title["title_type"].isin("movie", "tvSeries", "tvMiniSeries", "tvMovie")) \
#        .select(title["id"]) 
#
#category_filtered = category \
#        .where(category["name"] == "actress") \
#        .select(category["id"]) 
#
#name_filtered = name \
#        .filter(name["death_year"].isNull()) \
#        .select(name["id"], name["primary_name"], (year(current_timestamp()) - name["birth_year"]).alias("age"))       
#        
#
#title_filtered.alias("t") \
#        .join(titlePrincipals.alias("tp"), col("tp.title_id") == col("t.id")) \
#        .join(category_filtered.alias("c"), col("c.id") == col("tp.category_id")) \
#        .join(name_filtered.alias("n"), col("n.id") == col("tp.name_id")) \
#        .join(titleEpisode.alias("te"), col("te.title_id") == col("tp.title_id"), "left") \
#        .filter(col("te.title_id").isNull()) \
#        .join(titlePrincipalsCharacters.alias("tpc"), (col("tpc.title_id") == col("tp.title_id")) & (col("tpc.name_id") == col("tp.name_id"))) \
#        .groupBy(col("n.id"), col("n.primary_name"), col("age")) \
#        .agg(count("*").alias("roles")) \
#        .orderBy(col("roles").desc()) \
#        .limit(100) \
#        .show()
#        
#end = time.time()
#print("3.sql time: ", end - start)                    

    




