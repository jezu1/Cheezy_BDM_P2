# -*- coding: utf-8 -*-
"""
# Create datamart for dashboard
"""

import pyspark
from delta import *
import os
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.ml.feature import Bucketizer


#=============================================================================#
#                           SET UP SPARK SESSION                              #
#=============================================================================#

builder = pyspark.sql.SparkSession.builder.appName("MyApp")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()



#=============================================================================#
#                                PATH INPUTS                                  #
#=============================================================================#

path_landing="/user/hadoop/delta/cheezy_data"
path_dw="/user/hadoop/delta/warehouse"
path_save="/user/hadoop/delta/datamart"

# Create new directory for datamart
os.popen(f"hdfs dfs -mkdir -p {path_save}")

# Check creation of datamart directory
os.popen(f"hdfs dfs -ls /user/hadoop/delta").read().split('\n')

#=============================================================================#
#                                  FUNCTIONS                                  #
#=============================================================================#

def os_get_files(path):
    """
    given an hdfs path, list the files inside it
    """
    files=os.popen(f"hdfs dfs -ls {path}").read()
    files=[path+'/'+i.split(path+'/')[-1] for i in files.split('\n') if i.startswith('drwxr')]
    return files

def read_spark_df(tablename, path):
    """
    Read file as spark dataframe given a tablename and directory
    """
    complete_file=f"{path}/{tablename}"
    return spark.read.format("delta").load(f"hdfs://localhost:9000{complete_file}")

def save_delta_table(df, file, savepath=path_dw, overwriteSchema=False):
    """
    Save delta table.
    input:
        df: spark dataframe
        savepath: dw filepath with name of table
    """
    if overwriteSchema==True:
        df.write.format("delta")                .mode("overwrite")                .option("overwriteSchema", "True")                 .save("hdfs://localhost:9000"+savepath+'/'+file)        
    else:
        df.write.format("delta")                .mode("overwrite")                .save("hdfs://localhost:9000"+savepath+'/'+file)

def read_save_delta(src, trgt):
    """
    Save src file as trgt file
    """
    df= spark.read.format("delta").load(f"hdfs://localhost:9000{src}")
    df.write.format("delta")            .mode("overwrite")            .save("hdfs://localhost:9000"+trgt)
    print(f"Saved {src} to {trgt}")
    
def create_temp_spark_table(tablename, path):
    """
    # Read data and create temporary table. Good if using sparkSQL
    """
    spark.read.format("delta").load("hdfs://localhost:9000{path}/{tablename}").registerTempTable(tablename)

# Check what files are available in dw
os_get_files(path_dw)

#=============================================================================#
#                              AGGREGATE DATA                                 #
#=============================================================================#

# Read data

swipes=read_spark_df('swipes', path_dw).withColumn("date",to_date("timestamp")).drop(*['timestamp','id'])
images=read_spark_df('images', path_dw).select(*['id','file','dishId','restaurantId']).withColumnRenamed("id",'imageId')

swipes=swipes.join(images, on='imageId')

users=read_spark_df('users', path_dw).withColumnRenamed("id",'userId')
bucketizer = Bucketizer(splits=[i*5 for i in range(20)]+[float('Inf')],inputCol="age", outputCol="age_bucket")
users = bucketizer.setHandleInvalid("keep").transform(users).withColumn("age_bucket",col('age_bucket').cast('int'))

restaurants=read_spark_df('restaurants', path_dw).withColumnRenamed("key",'restaurantId')
dishes=read_spark_df('dishes', path_dw).select(*['id','name']).withColumnRenamed("id",'dishId')


# Get daily statistics per restaurant
daily_resto=(swipes.groupBy(*['restaurantId','date'])
             .agg(countDistinct("userId").alias("users"), \
                  count("swipeLeft").alias("impressions"), \
                  sum("swipeLeft").alias("left_swipes"),
                 )
            )
save_delta_table(daily_resto, 'daily_resto', savepath=path_save)


# Get monthly statistics per restaurant
monthly_resto=(daily_resto.withColumn("month", date_format('date','yyyy-MM'))
                 .groupBy(*['month','restaurantId'])
                 .agg(avg("users").alias("dly_avg_users"),
                      avg("impressions").alias("dly_avg_impressions"), 
                      avg("left_swipes").alias("dly_avg_left_swipes"), 
                      countDistinct("date").alias("days_active"),
                      )
               .join(restaurants.select(*['restaurantId','restaurant_name']), on='restaurantId')
               .sort(col("month").asc(),col("dly_avg_impressions").desc())
               .withColumn("rank", dense_rank().over(Window.partitionBy('month').orderBy(desc("dly_avg_impressions"))))
                    )
save_delta_table(monthly_resto, 'monthly_resto', savepath=path_save)


# Get top and bottom 3 images per month and per restaurant
monthly_resto_images=(swipes.withColumn("month", date_format('date','yyyy-MM'))
                         .groupBy(*['restaurantId','month','imageId','dishId'])
                         .agg(count("swipeLeft").alias("impressions"), \
                              sum("swipeLeft").alias("left_swipes"),
                             )
                      .sort(col("restaurantId").asc(),col("month").asc(),col("impressions").desc())
                        )
window1 = Window.partitionBy('restaurantId',"month").orderBy(col("impressions").desc())
window2 = Window.partitionBy('restaurantId',"month").orderBy(col("impressions").asc())
monthly_resto_images=(monthly_resto_images
     .withColumn("toprow",row_number().over(window1))
    .withColumn("botrow",row_number().over(window2))
     .filter((col('toprow')<4) | (col('botrow')<4))
     .join(dishes, on='dishId')
     .withColumn('rank',
            when(col("toprow") < col('botrow'), concat(lit("top-"),col('toprow')))
            .otherwise(concat(lit("bot-"),col('botrow')))
                )
    .join(images.select(*['imageId','file']), on='imageId')
    .drop('toprow','botrow','dishId','imageId')
    )

save_delta_table(monthly_resto_images, 'monthly_resto_images', savepath=path_save)


# Get user value counts per residence city and age group for each restaurnt monthly
monthly_resto_users=(swipes
                     .withColumn("month", date_format('date','yyyy-MM'))
                     .select(*['userId','restaurantId','month'])
                     .dropDuplicates()
                     .join(users.select(*['userId','city','age_bucket']), 
                           on='userId')
                     .groupBy(*['restaurantId','month','city','age_bucket'])
                     .agg(count('userId').alias('users'))
                     .sort(*['restaurantId','month','city','age_bucket'])
                    )
save_delta_table(monthly_resto_users, 'monthly_resto_users', savepath=path_save)

# CHECK OUTPUT IN DATAMART FOLDER
os_get_files(path_save)

