import pandas as pd
import pyspark
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import re
import os
import json


# convert the review text from the temporal landing zone into proper json format
def proper_json_reviews(spark_json):
    reviews = spark_json

    reviews = reviews.replace('"', "'")
    reviews = reviews.replace("', '", "\", \"")
    reviews = reviews.replace("\':", "\":")
    reviews = reviews.replace(": \'", ": \"")
    reviews = reviews.replace("{\'", "{\"")

    reviews = reviews.replace("\\", "")

    reviews = reviews.replace("}\n", "},\n")

    reviews = reviews.replace("False", "false")
    reviews = reviews.replace("True", "true")
    reviews = reviews.replace("None", "null")
    reviews = reviews.replace("null, '", "null, \"")

    pattern = r'(?<=[0-9], )\''
    reviews = re.sub(pattern, '"', reviews)

    json_reviews = json.loads(reviews)

    return json_reviews


# convert the opening hours from the temporal zone to proper json format and normalize them (as they are nested json)
def proper_json_opening_hours(spark_json):
    opening_hours = spark_json

    opening_hours = opening_hours.replace(", \'", ", \"")
    opening_hours = opening_hours.replace("\',", "\",")
    opening_hours = opening_hours.replace("\':", "\":")
    opening_hours = opening_hours.replace(": \'", ": \"")
    opening_hours = opening_hours.replace("{\'", "{\"")
    opening_hours = opening_hours.replace("\'}", "\"}")
    opening_hours = opening_hours.replace("\n", ",")

    json_hours = json.loads(opening_hours)
    df_hours = pd.json_normalize(json_hours)

    return df_hours

#get the data from the google places detail API call form the persistent landing zone
def get_google_details(spark):
    # get all the file pathes
    p = os.popen("hdfs dfs -ls /user/hadoop/delta/google_details").read()
    files = ['/user/' + i.split('/user/')[-1] for i in p.split('\n') if '/user/' in i and 'hadoop' in i]

    # get the schema of the delta files
    df = spark.read.format("delta").option("mergeSchema", "true").load(
        "hdfs://localhost:9000/user/hadoop/delta/google_details/g_ChIJ-4eEM3OYpBIRRMIYuy1s0HE")
    df = df.selectExpr("reviews", "`opening_hours.periods` as opening_periods")

    # sort the columns alphabetically (as their order can variey always sort so they match the schema)
    df = df.select(sorted(df.columns))
    schema_google = df.schema

    # create an empty spark DataFrame with the same schema
    df_google_all = spark.createDataFrame([], schema_google)

    # list for the google keys
    google_keys = []

    # loop through all the files in the folder
    for file_name in files:

        # google restaurant key is in the filename
        google_key = os.path.basename(file_name)

        try:
            # read delta table
            df_google_file = spark.read.format("delta").load("hdfs://localhost:9000" + file_name)

            # check wich columns are present (not all restaurants have reviews etc.) and select according to this
            if 'reviews' in df_google_file.columns and 'opening_hours.periods' in df_google_file.columns:
                df_google_file = df_google_file.selectExpr("reviews", "`opening_hours.periods` as opening_periods")

            elif 'reviews' in df_google_file.columns:
                df_google_file = df_google_file.withColumn("opening_periods", lit(None).cast("string"))
                df_google_file = df_google_file.selectExpr("reviews", "opening_periods")

            elif 'opening_hours.periods' in df_google_file.columns:
                df_google_file = df_google_file.withColumn("reviews", lit(None).cast("string"))
                df_google_file = df_google_file.selectExpr("reviews", "`opening_hours.periods` as opening_periods")

            # sort the columns
            df_google_file = df_google_file.select(sorted(df.columns))
            # Union the files
            df_google_all = df_google_all.coalesce(1).union(df_google_file)

            # append the key to a list to later create a column from this list
            google_keys.append(google_key)

        except Exception as e:
            print(file_name)
            print("An exception occurred:", str(e))

        # free the memory
        df_google_file.unpersist()

    # Create a new column with unique IDs
    df_google_all = df_google_all.withColumn("id", monotonically_increasing_id())

    # do the same for the google_keys
    keys_df = spark.createDataFrame([(key,) for key in google_keys], ["google_key"])
    keys_df = keys_df.withColumn("id", monotonically_increasing_id())

    # join by the just created ids and drop them (now every restaurant is assigned the correct google key)
    df_google_all = df_google_all.join(keys_df, "id", "inner").drop("id")

    df_google_all = df_google_all.selectExpr("google_key", "reviews", "opening_periods")

    return df_google_all


def get_reviews(spark,df_google_details):
    # create schema for the reviews dataframe
    schema = StructType([
        StructField("rating", IntegerType(), nullable=True),
        StructField("text", StringType(), nullable=True),
        StructField("time", LongType(), nullable=True),
        StructField("google_key", StringType(), nullable=False)
    ])
    # create empty spark dataframe
    combined_df_reviews = spark.createDataFrame([], schema)

    #loop through the dataframe and unwind the reviews as they are in json format (most of the time 5 reviews per json)
    for row in df_google_details.itertuples():
        try:
            if row.reviews is not None:
                # format the string
                json_reviews = proper_json_reviews(row.reviews)

                # create spark dataframe form json string
                df_reviews = spark.createDataFrame(json_reviews)

                # select the needed columns
                df_reviews = df_reviews.select("rating", "text", "time")
                # add the google restaurant key so it can be identified to which restaurant the review belongs
                df_reviews = df_reviews.withColumn('google_key', lit(row.google_key))

                # merge all reviews together to one table
                combined_df_reviews = combined_df_reviews.coalesce(1).union(df_reviews)

                # free the memory
                df_reviews.unpersist()

        except Exception as e:
            print(row.google_key)
            print("An exception occurred:", str(e))

    #get google key for join with restaurant data
    combined_df_reviews = combined_df_reviews.withColumn("google_key", substring("google_key", 3, 99999))
    combined_df_reviews = combined_df_reviews.withColumnRenamed("rating", "google_rating")

    return combined_df_reviews

def create_reviews(spark , combined_df_reviews):
    #get the restaurants from the already created datawarehouse table
    df_restaurants = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

    #join the restaurants with the reviews by google key
    reviews_tabel = combined_df_reviews.join(df_restaurants, trim(combined_df_reviews.google_key) == trim(df_restaurants.go_key),"inner") \
                    .selectExpr("key as ra_key", "time", "google_rating as rating", "text")

    #add and adjust columns
    reviews_tabel = reviews_tabel.withColumn("source", lit("google"))
    reviews_tabel = reviews_tabel.withColumn("sample", lit(None))
    reviews_tabel = reviews_tabel.withColumn("review_key", concat(lit("g_"), monotonically_increasing_id()))
    reviews_tabel = reviews_tabel.withColumn("rating", col("rating").cast("string"))

    reviews_tabel = reviews_tabel.selectExpr("ra_key", "review_key", "time", "rating", "sample", "text", "source")

    #append to the already existing review table (table already contains the tripadvisor reviews)
    reviews_tabel.write.format("delta").mode("append").save("hdfs://localhost:9000/user/hadoop/delta/warehouse/long_review")


def get_opening_hours(spark, df_google_details):
    # schema for the pyspark dataframe
    schema = StructType([
        StructField("day", IntegerType(), nullable=True),
        StructField("open_time", IntegerType(), nullable=True),
        StructField("close_time", IntegerType(), nullable=True),
        StructField("google_key", StringType(), nullable=False)
    ])
    combined_df_opening_hours = spark.createDataFrame([], schema)

    #loop through the dataframe and unwind the opening hours (they are in nested json format)
    for row in df_google_details.itertuples():
        try:
            #check if opening hours are known
            if row.opening_periods != None:
                #unwind json
                df_hours = proper_json_opening_hours(row.opening_periods)

                #create spark dataframe
                df_opening_hours = spark.createDataFrame(df_hours)
                df_opening_hours = df_opening_hours.selectExpr("`close.day` as day", "`open.time` as open_time",
                                                               "`close.time` as close_time")
                df_opening_hours = df_opening_hours.withColumn('google_key', lit(row.google_key))

                #union with opening hours of previous iterations
                combined_df_opening_hours = combined_df_opening_hours.coalesce(1).union(df_opening_hours)
                #free memory
                df_opening_hours.unpersist()

        except:
            print(row.google_key)
            print("An exception occurred:", str(e))

    #remove g_ prefix from google key
    combined_df_opening_hours = combined_df_opening_hours.withColumn("google_key", substring("google_key", 3, 99999))
    return combined_df_opening_hours


def create_opening_hours(spark, combined_df_opening_hours):
    #get the restaurants from the datawarehouse
    df_restaurants = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

    #join them with the opening hours by google key
    opening_hours_tabel = combined_df_opening_hours.join(df_restaurants,\
                        combined_df_opening_hours.google_key == df_restaurants.go_key,"inner") \
                        .selectExpr("key as ra_key", "day as day_key", "open_time", "close_time")

    #write opening hours to datawarehouse
    opening_hours_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/opening_hours")

#create a datawarehouse table for the weekdays because in the google data weekdays are identified by numbers from 1-6
def create_weekday(spark):
    # Define the data
    weekdays = [
        (0, "Sunday"),
        (1, "Monday"),
        (2, "Tuesday"),
        (3, "Wednesday"),
        (4, "Thursday"),
        (5, "Friday"),
        (6, "Saturday"),
    ]

    weekday_tabel = spark.createDataFrame(weekdays, ["day_key", "weekday"])

    weekday_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/weekday")


def main():

    #create spark session
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df_google_all = get_google_details(spark)
    #transform to pandas because looping for reviews and opening hours is faster
    df_google_details = df_google_all.toPandas()
    #create review table
    df_reviews = get_reviews(spark,df_google_details)
    create_reviews(spark,df_reviews)
    #create opening hours table
    df_opening_hours = get_opening_hours(spark, df_google_details)
    create_opening_hours(spark, df_opening_hours)
    create_weekday(spark)
