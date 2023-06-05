import pandas as pd
import pyspark
from delta.tables import *
from pyspark.sql.functions import *
from delta import *
import re
import os


#get all the images from the hadoop directory and put them and their google key in a pyspark dataframe
def get_google_images(spark):
    # get all the google images from hadoop
    p = os.popen("hdfs dfs -ls /user/hadoop/google_images").read()
    images = ['/user/' + i.split('/user/')[-1] for i in p.split('\n') if '/user/' in i and 'hadoop' in i]

    image_files = []
    google_keys = []

    # get the google restaurant key (is in the filename of the image)
    for image in images:
        filename = os.path.basename(image)
        match = re.search(r"(.+)_\d+\.jpg", filename)
        if match:
            google_key = match.group(1)
            image_files.append(image)
            google_keys.append(google_key)

        else:
            print(image)

    google_image_data = [(google_key, image_file) for google_key, image_file in zip(google_keys, image_files)]

    # Define the schema for the pyspark DataFrame
    schema = StructType([
        StructField("google_key", StringType(), True),
        StructField("image_file", StringType(), True)
    ])

    # create the spark dataframe
    df_google_images = spark.createDataFrame(google_image_data, schema)
    #remove the g_ prefix
    df_google_images = df_google_images.withColumn("google_key", substring("google_key", 3, 100))
    df_google_images = df_google_images.withColumn("source", lit("google"))

    return df_google_images


def main():
    #build spark session
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    #get the images
    df_google_images = get_google_images(spark)

    #get the restaurant from the warehouse
    df_restaurants = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
    #join the images wth the restaurants by the google key
    googel_image_tabel = df_google_images.join(df_restaurants, df_google_images.google_key == df_restaurants.go_key,"inner") \
                                            .selectExpr("key as ra_key", "image_file", "source")

    # write the images (filepath) not the image itself to the data warehouse
    googel_image_tabel.write.format("delta").mode('append').save(
        "hdfs://localhost:9000/user/hadoop/delta/warehouse/images")
