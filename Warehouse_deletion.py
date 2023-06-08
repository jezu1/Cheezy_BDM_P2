import pandas as pd
import pyspark
from delta.tables import *
from pyspark.sql.functions import *
import pyarrow.fs as fs
from delta import *
import os
import geopandas as gpd
import numpy as np



def remove_restaurants(df_closed,df_restaurants):
    #only those restaurants not in the closed dataframe remain in the dataframe
    df_closed_restaurants = df_restaurants.join(df_closed, df_restaurants["go_key"] == df_closed["place_id"], "left_anti")

    #get the general keys of the deleted restaurants
    df_delete = df_restaurants.join(df_closed, df_restaurants.go_key == df_closed.place_id, "inner").select("key")

    #overwrite current restaurant table with the data without the deleted restaurants
    df_closed_restaurants.write.format("delta").mode("overwrite").save("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

    #return the keys of the related restaurants to remove them form the other tables
    return df_delete

def remove_restaurants_from_tables(spark, df_deleted):

    #get keys to string list to be able to use sql "in" operator
    key_deleted_list = df_deleted.select("key").distinct().rdd.flatMap(lambda x: x).collect()
    values_str = ", ".join([f"'{value}'" for value in key_deleted_list])
    #delte closed restaurants from data warehouse tables
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/cuisines` WHERE restaurant_key IN ({values_str})")
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/meals` WHERE restaurant_key IN ({values_str})")
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/diets` WHERE restaurant_key IN ({values_str})")
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/short_review` WHERE ra_key IN ({values_str})")
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/long_review` WHERE ra_key IN ({values_str})")
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/opening_hours` WHERE ra_key IN ({values_str})")
    spark.sql(
        f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/images` WHERE ra_key IN ({values_str})")
    spark.sql(
            f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/michelin` WHERE ra_key IN ({values_str})")
    spark.sql(
            f"DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/rating` WHERE ra_key IN ({values_str})")


def main():
    #create spark session
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    #get keys of all the closed restaurants
    df_closed = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants").filter("closed = 1")
    df_closed = df_closed.select("key")

    #delete all restaurants that have the closed flag from the database

    spark.sql("""
        DELETE FROM delta.`hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants`
        WHERE closed = 1
    """)

    #remove the data corresponding to those restaurants from the other tables
    remove_restaurants_from_tables(spark, df_closed)
