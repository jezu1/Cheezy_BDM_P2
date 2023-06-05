import pandas as pd
import pyspark
from delta.tables import *
from pyspark.sql.functions import *
import pyarrow.fs as fs
from delta import *
import re
import os
import json
import traceback
import geopandas as gpd
import numpy as np

import random
from builtins import min

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType


def prepare_tripadvisor(df_tripadvisor):
    #add prefix ta to tripadvisor columns
    new_columns = ["ta_" + col for col in df_tripadvisor.columns]
    for old_col, new_col in zip(df_tripadvisor.columns, new_columns):
        df_tripadvisor = df_tripadvisor.withColumnRenamed(old_col, new_col)

    #get tripadvisor key from the url
    df_tripadvisor = df_tripadvisor.withColumn("ta_id", regexp_extract(df_tripadvisor.ta_url, r"g(\d+)-d(\d+)", 0))

    return df_tripadvisor

#get all the google places api data from the persistent landing zone
def get_google_table(spark):
    # get list of all google basic files
    p = os.popen("hdfs dfs -ls /user/hadoop/delta/google_rest_clean").read()
    files = ['/user/' + i.split('/user/')[-1] for i in p.split('\n') if '/user/' in i and 'hadoop' in i]

    # get the schema of the delta files
    df = spark.read.format("delta").option("mergeSchema", "true").load(
        "hdfs://localhost:9000/user/hadoop/delta/google_rest_clean/google1")
    df = df.selectExpr("name", "place_id", "price_level", "rating", "user_ratings_total" \
                       , "vicinity", "`geometry.location.lat` as latitude", "`geometry.location.lng` as longitude")

    # sort the schema alphabetically because otherwise no load possible
    df = df.select(sorted(df.columns))
    schema_google = df.schema

    # create an empty DataFrame with the same schema
    df_google_all = spark.createDataFrame([], schema_google)

    # loop through all the files
    for file_name in files:
        df_google_file = spark.read.format("delta").load("hdfs://localhost:9000" + file_name)
        try:
            # select the needed columns
            df_google_file = df_google_file.selectExpr("name", "place_id", "price_level", "rating", "user_ratings_total" \
                                                       , "vicinity", "`geometry.location.lat` as latitude",
                                                       "`geometry.location.lng` as longitude")

            # sort the columns alphabetically
            df_google_file = df_google_file.select(sorted(df.columns))

            # union the dataframes
            df_google_all = df_google_all.coalesce(1).union(df_google_file)

        except:
            print(file_name)

        # free the memory
        df_google_file.unpersist()

    df_google_all = df_google_all.dropDuplicates(['place_id'])

    return df_google_all

#join the tripadvisor and google data by spatial join
#long and lat don't match exactly so assumption is when the restaurants are almost in the same place they are the same
def join_google_tripadvisor(df_google_all, df_tripadvisor):

    #convert both dataframes to pandas to by able to use geopandas for spatial join
    df_p_google = df_google_all.toPandas()
    df_p_tripadvisor = df_tripadvisor.toPandas()

    # Convert tipadvisor to geo dataframe with point from latitude and longitude as geometry column
    geometry = gpd.points_from_xy(df_p_tripadvisor['ta_longitude'], df_p_tripadvisor['ta_latitude'])
    trip_geo_df = gpd.GeoDataFrame(df_p_tripadvisor, crs={'init': 'epsg:4326'}, geometry=geometry)

    # draw a 7 meter radius around every point
    trip_geo_df_utm33N = trip_geo_df.to_crs(crs="+proj=utm +zone=33 +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
    trip_geo_df_utm33N['buffer_geometry'] = trip_geo_df_utm33N.geometry.buffer(7)

    #make the area created by the radius the geometry column
    trip_geo_df_utm33N = trip_geo_df_utm33N.rename(
        columns={'geometry':'original_geometry', 'buffer_geometry':'geometry'}).set_geometry('geometry')

    # Convert the latitude and longitude from google to points and create a geo dataframe
    geometry = gpd.points_from_xy(df_p_google['longitude'], df_p_google['latitude'])
    google_geo_df = gpd.GeoDataFrame(df_p_google, crs={'init': 'epsg:4326'}, geometry=geometry)

    google_geo_df = google_geo_df.to_crs(epsg=32633)

    #perform the spatial join
    df_joined_rest = gpd.sjoin(google_geo_df, trip_geo_df_utm33N, op='within', how='inner')

    return df_joined_rest

#create the main restaurant table in the data warehouse
def create_restaurants(spark_joined_rest):
    #select columns
    restaurant_tabel = spark_joined_rest.selectExpr("key", "place_id as go_key", "ta_id as ta_key",
                                                    "name as restaurant_name", \
                                                    "price_level as g_price", "ta_price", "vicinity as address",
                                                    "latitude", \
                                                    "longitude", "ta_website as website", "ta_email as email",
                                                    "ta_telephone as telephone")

    #get the price as numeric value
    restaurant_tabel = restaurant_tabel.withColumn("ta_price_level",
                                                   when(col("ta_price") == "$", 2)
                                                   .when(col("ta_price") == "$$ - $$$", 3)
                                                   .when(col("ta_price") == "$$$$", 4)
                                                   .otherwise(None))

    #combine the price
    restaurant_tabel = restaurant_tabel.withColumn("price", coalesce(restaurant_tabel["g_price"],
                                                                     restaurant_tabel["ta_price_level"].cast("double")))

    restaurant_tabel =restaurant_tabel.drop("ta_price","ta_price_level","g_price")

    #write table to data warehouse
    restaurant_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

#get the cuisines from tripadvisor
def create_cuisines(spark_joined_rest):
    cuisine_tabel = spark_joined_rest.selectExpr("key as restaurant_key", "ta_cuisine as cuisine")
    #unwind the cuisines array
    cuisine_tabel = cuisine_tabel.withColumn("cuisine", explode(split(cuisine_tabel.cuisine, ",")))
    cuisine_tabel = cuisine_tabel.withColumn("cuisine", trim(col("cuisine")))

    #write to data warehouse
    cuisine_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/cuisines")

#get the meals from tripadvisor
def create_meals(spark_joined_rest):
    meals_tabel = spark_joined_rest.selectExpr("key as restaurant_key", "ta_meals as meals")
    #unwind the meals array
    meals_tabel = meals_tabel.withColumn("meals", explode(split(meals_tabel.meals, ",")))
    meals_tabel = meals_tabel.withColumn("meals", trim(col("meals")))
    #write to data warehouse
    meals_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/meals")

#get the diets from tripadvisor
'''
def create_diets(spark_joined_rest):
    diets_tabel = spark_joined_rest.selectExpr("key as restaurant_key", "ta_special_diets as diets")
    #unwind the diets array
    diets_tabel = diets_tabel.withColumn("diets", explode(split(diets_tabel.diets, ",")))
    diets_tabel = diets_tabel.withColumn("diets", trim(col("diets")))
    #write to data warehouse
    diets_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/diets")
'''

#get the short reviews from tripadvisor
'''
def create_short_reviews(spark_joined_rest):
    short_review_tabel = spark_joined_rest.selectExpr("key as restaurant_key", "ta_review_preview as short_review")
    #unwind the short reviews array
    short_review_tabel = short_review_tabel.withColumn("short_review",
                                                       explode(split(short_review_tabel.short_review, "',")))
    #remove unnecessary characters
    short_review_tabel = short_review_tabel.withColumn("short_review", trim(col("short_review")))
    short_review_tabel = short_review_tabel.withColumn("short_review",
                                                       regexp_replace(col("short_review"), "[\\[\\]]", ""))

    #wrote to data warehouse
    short_review_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/short_review")

'''

def create_long_reviews(spark, spark_joined_rest):
    #read data from kaggle dataset with reviews of Barcelona restaurants
    df_ba_rev = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/kaggle_data/Barcelona_reviews")

    df_url = spark_joined_rest.selectExpr("key as ra_key", "ta_url as url")

    #join the reviews with the restaurants by URL
    df_long_reviews = df_url.join(df_ba_rev, df_url.url == df_ba_rev.url_restaurant, "inner") \
        .selectExpr("ra_key", "_c0 as review_key", "url", "url_restaurant", "date", "rating_review", "sample",
                    "title_review", "review_full") \
        .orderBy("ra_key")

    df_long_reviews = df_long_reviews.withColumn("review_key", concat(lit("t_"), col("review_key")))
    #transform date to timestamp
    df_long_reviews = df_long_reviews.withColumn("time", unix_timestamp(df_long_reviews.date, "MMMM d, yyyy"))
    long_reviews_tabel = df_long_reviews.selectExpr("ra_key","review_key","time","rating_review as rating","sample","review_full as text")
    #add source column with source tripadvisor
    long_reviews_tabel = long_reviews_tabel.withColumn("source", lit("tripadvisor"))

    #write to data warehouse
    long_reviews_tabel.write.format("delta").mode('overwrite').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/long_review")

def create_michelin(spark, spark_joined_rest):
    #read kaggle michelin data from delta lake
    df_michelin = spark.read.format("delta").load(
        "hdfs://localhost:9000/user/hadoop/delta/kaggle_data/michelin_my_maps")

    df_michelin = df_michelin.filter(df_michelin.Location == "Barcelona").select("Name", "Address", "Location",
                                                                                 "MinPrice", "MaxPrice", "Cuisine",
                                                                                 "Award")
    #get the number of michelin stars
    df_michelin = df_michelin.withColumn("Award_Number", when(df_michelin.Award == "Bib Gourmand", -1)\
                                            .otherwise(regexp_extract(df_michelin.Award, r"(\d+)", 1).cast("int")))\
                                            .selectExpr("Name as rest_name", "Address",
                                                        "MinPrice", "MaxPrice", "Cuisine",
                                                        "Award_Number", "Award")

    #join michelin stars with the restaurant data
    michelin_tabel = df_michelin.join(spark_joined_rest, df_michelin.rest_name == spark_joined_rest.name, "inner") \
        .selectExpr("key as ra_key", "Award_Number as number_of_stars")

    michelin_tabel.write.format("delta").mode('overwrite').save("hdfs://localhost:9000/user/hadoop/delta/warehouse/michelin")

def create_ratings(spark_joined_rest):
    #get the ratings from tripadvisor
    df_ratings_trip = spark_joined_rest.selectExpr("key as ra_key", "ta_rating as rating").filter(col("ta_rating") != "NaN")
    df_ratings_trip = df_ratings_trip.withColumn("source", lit("Tripadvisor"))
    # Add a new column with the current timestamp rounded to the day
    df_ratings_trip = df_ratings_trip.withColumn("timestamp", unix_timestamp(date_trunc("day", current_timestamp())))

    #get the ratings from google
    df_ratings_google = spark_joined_rest.selectExpr("key as ra_key", "rating").filter(col("rating") != "NaN")
    df_ratings_google = df_ratings_google.withColumn("source", lit("google"))
    # Add a new column with the current timestamp rounded to the day
    df_ratings_google = df_ratings_google.withColumn("timestamp",
                                                     unix_timestamp(date_trunc("day", current_timestamp())))

    #union the rationgs into one table
    rating_tabel = df_ratings_google.union(df_ratings_trip)

    #write ratings to data warehouse
    rating_tabel.write.format("delta").mode("overwrite").save("hdfs://localhost:9000/user/hadoop/delta/warehouse/rating")

def main():

    # schema for pyspark dataframe of google and tripadvisor join
    schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("place_id", StringType(), True),
        StructField("price_level", DoubleType(), True),
        StructField("rating", DoubleType(), True),
        StructField("user_ratings_total", IntegerType(), True),
        StructField("vicinity", StringType(), True),
        StructField("ta_rank", DoubleType(), True),
        StructField("ta_name", StringType(), True),
        StructField("ta_url", StringType(), True),
        StructField("ta_rating", DoubleType(), True),
        StructField("ta_n_reviewers", DoubleType(), True),
        StructField("ta_cuisine", StringType(), True),
        StructField("ta_price", StringType(), True),
        StructField("ta_review_preview", StringType(), True),
        StructField("ta_image", StringType(), True),
        StructField("ta_rating_food", DoubleType(), True),
        StructField("ta_rating_service", DoubleType(), True),
        StructField("ta_rating_value", DoubleType(), True),
        StructField("ta_address", StringType(), True),
        StructField("ta_gmaps_url", StringType(), True),
        StructField("ta_latitude", DoubleType(), True),
        StructField("ta_longitude", DoubleType(), True),
        StructField("ta_website", StringType(), True),
        StructField("ta_email", StringType(), True),
        StructField("ta_telephone", StringType(), True),
        StructField("ta_cuisines", StringType(), True),
        StructField("ta_meals", StringType(), True),
        StructField("ta_price_range", StringType(), True),
        StructField("ta_special_diets", StringType(), True),
        StructField("ta_rating_atmosphere", DoubleType(), True),
        StructField("ta_features", StringType(), True),
        StructField("ta_id", StringType(), True)
    ])

    #create spark session
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    df_tripadvisor = spark.read.format("delta").load(
        "hdfs://localhost:9000/user/hadoop/delta/tripadvisor_rest/tripadvisor_plus")

    #prepare tripadvisor data
    df_tripadvisor = prepare_tripadvisor(df_tripadvisor)
    #get google data
    df_google = get_google_table(spark)
    #join the two dataframes
    df_joined_rest = join_google_tripadvisor(df_google,df_tripadvisor)

    #create spark dataframe from the joined dataframe
    spark_joined_rest = spark.createDataFrame(df_joined_rest,schema)
    #give id
    spark_joined_rest = spark_joined_rest.withColumn("key", monotonically_increasing_id())

    #create tables and write to datawarehouse
    create_restaurants(spark_joined_rest)
    create_diets(spark_joined_rest)
    create_meals(spark_joined_rest)
    create_cuisines(spark_joined_rest)
    create_short_reviews(spark_joined_rest)
    create_long_reviews(spark,spark_joined_rest)
    create_michelin(spark,spark_joined_rest)
    create_ratings(spark_joined_rest)