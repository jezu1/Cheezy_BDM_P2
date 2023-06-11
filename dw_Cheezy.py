# -*- coding: utf-8 -*-
"""
Move cheezy data to data warehouse
"""

import pyspark
from delta import *
import os
from pyspark.sql.functions import *

#=============================================================================#
#                           SET UP SPARK SESSION                              #
#=============================================================================#


# Set up spark session
builder = pyspark.sql.SparkSession.builder.appName("MyApp")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


#=============================================================================#
#                                PATH INPUTS                                  #
#=============================================================================#

path_landing="/user/hadoop/delta/cheezy_data"
path_dw="/user/hadoop/delta/warehouse"
path_dump='/user/hadoop/cheezy_data'

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

# Check what files are available in landing zone
os_get_files(path_landing)


#=============================================================================#
#                              ANONYMIZE USERS                                #
#=============================================================================#
# Split user to two datasets

# Read delta table
df=read_spark_df('users', path_landing)

# derive columns
df = (df
       .withColumn('city', split(split(df['address'], '\n').getItem(1), ', ').getItem(0))
       .withColumn('zipcode', split(split(df['address'], '\n').getItem(1), ', ').getItem(1))
       .withColumn('age', floor(datediff(current_date(), to_date(col('birthdate'), 'yyyy-M-d'))/365.25))
      )

# Subset columns
pii = df.select(*['id', 'username',
                  'name',
                  'address',
                  'mail',
                  'birthdate',
                  'phone',
                  'homeLat',
                  'homeLng'])

df = df.select(*['id',
                 'sex',
                 'createDate',
                 'interestedCuisines',
                 'nationality',
                 'occupation',
                 'city',
                 'zipcode',
                 'age'])

save_delta_table(df, 'users', savepath=path_landing, overwriteSchema=True)
save_delta_table(pii, 'users_pii', savepath=path_dw)


#=============================================================================#
#                                  NORMALIZE                                  #
#=============================================================================#
# normalize and remove columns with arrays types before saving to data warehouse

def normalize(file,
              listcol,
              name_rel_table,
              indexcol='id',
              path=path_landing,
             savepath=path_dw,
             ):
    """
    Input:
        file: name of delta table in landing zone
        name_rel_table: new table name of relationships/edge table
        indexcol: id column name of delta table being read
        path: path of landing zone
        savepath: path to save new tables
    """

    df=read_spark_df(file, path)

    singular={'dishes':'dish','images':'image','locations':'location',
                  'reviews':'review','swipes':'swipe','users':'user',
                 'tags':'tag','ingredients':'ingredient','interestedCuisines':'interestedCuisine'
                 }

    id_name=singular[file]+'Id'
    new_name=singular[listcol]+'Id'

    # unique values
    df2 = (df.select(explode(df[listcol]))
           .dropDuplicates()
           .withColumn("id",monotonically_increasing_id()+1)
           .withColumnRenamed("col",singular[listcol]))

    # Table of relationships/edges
    df3=(df.select(df[indexcol],explode(df[listcol]))
     .dropDuplicates()
     .withColumnRenamed("col",singular[listcol])
     .withColumnRenamed("id",id_name)
     .join(df2.withColumnRenamed("id",new_name), 
           on=singular[listcol]).drop(singular[listcol])
    )
    
    # edit df to remove normalized column
    df = df.drop(listcol)
    
    # get restaurant id
    if file=='images':
        dishes=read_spark_df('dishes', path).select(*['id','restaurantId']).withColumnRenamed("id",'dishId')
        df=df.join(dishes, on='dishId')

    # save files
    save_delta_table(df, file, savepath=savepath)
    save_delta_table(df2, listcol, savepath=savepath)
    save_delta_table(df3, name_rel_table, savepath=savepath)
    print(f'Saved all files to {savepath}: {file}, {listcol}, {name_rel_table}')

# CALL NORMALIZATION FUNCTIONS
# Ingredients in dishes
normalize(file='dishes',
              listcol='ingredients',
              name_rel_table='ingredients_in_dish',
              indexcol='id',
              path=path_landing,
             savepath=path_dw,
             )

# Tags per image
normalize(file='images',
              listcol='tags',
              name_rel_table='image_tags',
              indexcol='id',
              path=path_landing,
             savepath=path_dw,
             )

# Cuisines users are interested in
normalize(file='users',
              listcol='interestedCuisines',
              name_rel_table='user_cuisines',
              indexcol='id',
              path=path_landing,
             savepath=path_dw,
             )


#=============================================================================#
#                            DIRECT TRANSFER                                  #
#=============================================================================#
# Check dw and cheezy files

files_dw=os_get_files(path_dw)
print('Data warehouse:',files_dw)

files_landing=os_get_files(path_landing)
print('Cheezy landing:',files_landing)


# Get list of files to transfer
transfer={i:path_dw+'/'+i.split('/')[-1] for i in files_landing if path_dw+'/'+i.split('/')[-1] not in files_dw}

# Iterate per file
for src in transfer.keys():
    read_save_delta(src, trgt=transfer[src])

# Check Data Warehouse for all saved outputs
os_get_files(path_dw)

