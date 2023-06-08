import pandas as pd
import pyspark
from delta.tables import *
from pyspark.sql.functions import *
import pyarrow.fs as fs
from delta import *
import os
import geopandas as gpd
import numpy as np


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
    df = df.selectExpr("name", "place_id","business_status","price_level", "rating", "user_ratings_total" \
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


#df restaurant long and lat cme from google ? Yes
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
    df_joined_rest = gpd.sjoin(google_geo_df, trip_geo_df_utm33N, op='within', how='left')

    return df_joined_rest

def mark_closed(df_closed,df_restaurants):

    # Perform a left join to match the restaurants in df_restaurants with the closed restaurants in df_closed
    joined_df = df_restaurants.join(df_closed, df_restaurants["go_key"] == df_closed["place_id"], how="left")
    # Update the "closed" column to 1 where there is a match, otherwise set it to 0
    joined_df = joined_df.withColumn("closed", when(df_closed["key"].isNull(), 0).otherwise(1))
    #add the closing time
    # Add the closed_time column
    joined_df = joined_df.withColumn("closed_time", when(col("closed") == 1, current_timestamp()).otherwise(lit(None)))
    # Select the columns from df_restaurants to ensure the same schema
    result_df = joined_df.select(df_restaurants.columns)

    # Write to restaurant table
    result_df.write.format("delta").mode("overwrite").save("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

def update_restaurants(spark,spark_joined_rest):
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

    #update data warehouse
    spark.sql("""
        MERGE INTO delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants` AS target
        USING restaurant_tabel
        ON target.key = restaurant_tabel.key
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)


#get the cuisines from tripadvisor
def update_cuisines(spark, spark_joined_rest):

    # Retrieve the distinct meal keys and their cuisine from the database table
    cuisine_mappings = spark.sql("""
        SELECT DISTINCT cuisine_key, cuisine
        FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/cuisines`
    """).collect()
    #create a mapping to assign the corrext meal keys to the meals
    cuisine_mapping = {row["cuisine"]: row["cuisine_key"] for row in cuisine_mappings}

    cuisine_tabel = spark_joined_rest.selectExpr("key as restaurant_key", "ta_cuisine as cuisine")
    #unwind the cuisines array
    cuisine_tabel = cuisine_tabel.withColumn("cuisine", explode(split(cuisine_tabel.cuisine, ",")))
    cuisine_tabel = cuisine_tabel.withColumn("cuisine", trim(col("cuisine")))

    # update data warehouse
    spark.sql("""
        MERGE INTO delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/meals` AS target
        USING (
            SELECT 
                restaurant_key, 
                cuisine,
                CASE 
                    """ + " ".join(
                    ["WHEN trim(cuisine) = '{}' THEN {}".format(cuisine, key) for cuisine, key in cuisine_mapping.items()]) + """
                    ELSE -1 -- Default value
                END AS cuisine_key
            FROM cuisine_tabel
        ) cuisine_data
        ON target.restaurant_key = cuisine_data.restaurant_key
        WHEN MATCHED THEN
            UPDATE SET target.restaurant_key = cuisine_data.restaurant_key,
                        target.cuisine = cuisine_data.cuisine
        WHEN NOT MATCHED THEN
            INSERT (meal_key, restaurant_key, meals)
            VALUES (cuisine_data.cuisine_key, cuisine_data.restaurant_key, cuisine_data.cuisine)
    """)


#get the meals from tripadvisor
def update_meals(spark,spark_joined_rest):

    # Retrieve the distinct meal keys and their meal text from the database table
    meal_mappings = spark.sql("""
        SELECT DISTINCT meal_key, meals
        FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/meals`
    """).collect()
    #create a mapping to assign the corrext meal keys to the meals
    meal_mapping = {row["meals"]: row["meal_key"] for row in meal_mappings}

    #prepare the new meal data
    meals_tabel = spark_joined_rest.selectExpr("key as restaurant_key", "ta_meals as meals")
    #unwind the meals array
    meals_tabel = meals_tabel.withColumn("meals", explode(split(meals_tabel.meals, ",")))
    meals_tabel = meals_tabel.withColumn("meals", trim(col("meals")))

    # update data warehouse
    spark.sql("""
        MERGE INTO delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/meals` AS target
        USING (
            SELECT 
                restaurant_key, 
                meals,
                CASE 
                    """ + " ".join(
                    ["WHEN trim(meals) = '{}' THEN {}".format(meal, key) for meal, key in meal_mapping.items()]) + """
                    ELSE -1 -- Default value
                END AS meal_key
            FROM meals_table
        ) meals_data
        ON target.restaurant_key = meals_data.restaurant_key
        WHEN MATCHED THEN
            UPDATE SET target.restaurant_key = meals_data.restaurant_key,
                        target.meals = meals_data.meals
        WHEN NOT MATCHED THEN
            INSERT (meal_key, restaurant_key, meals)
            VALUES (meals_data.meal_key, meals_data.restaurant_key, meals_data.meals)
    """)


def update_ratings(spark,spark_joined_rest):
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
    rating_tabel.write.format("delta").mode("append").save("hdfs://localhost:9000/user/hadoop/delta/warehouse/rating")

    # update data warehouse
    spark.sql("""
        MERGE INTO delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/rating` AS target
        USING rating_tabel
        ON target.ra_key = rating_tabel.ra_key
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)


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

    #read restaurant table
    df_restaurants = spark.read.format("delta").load(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

    #mark restaurants as closed
    df_closed = df_google.filter(df_google.business_status == "CLOSED_PERMANENTLY").select("place_id")
    mark_closed(df_closed, df_restaurants)


    #update and append the restaurants that are open
    # get the currently highest id to increase it
    max_id = df_restaurants.agg({"id": "max"}).collect()[0][0]

    df_google = df_google.filter(df_google.business_status != "CLOSED_PERMANENTLY")
    df_joined_rest = join_google_tripadvisor(df_google, df_tripadvisor)

    df_update = df_joined_rest.join(df_restaurants, df_joined_rest.place_id == df_restaurants.go_key,"left") \
                .select(df_joined_rest["*"], df_restaurants["key"])

    df_update = df_update.withColumn("key", when(isNull(df_update["key"]),\
                                    monotonically_increasing_id() + max_id + 1)\
                                     .otherwise(df_update["key"]))

    #update the datawarehouse
    update_restaurants(spark,df_update)
    update_meals(spark,df_update)
    update_cuisines(spark,df_update)
    update_ratings(spark,df_update)





