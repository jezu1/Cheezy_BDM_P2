#!/usr/bin/env python
# coding: utf-8

# #### Importing Libraries and creating the connection

# In[1]:


#!pip install neo4jupyter
#!pip install py2neo
#pip install delta-spark
#pip install neo4j


# In[2]:


import pandas as pd
import pyspark
from delta.tables import *
from pyspark.sql.functions import *
import pyarrow.fs as fs
from delta import *
import re
import os
import json
from pyspark.sql import SparkSession
from neo4j import GraphDatabase
from py2neo import Graph
import logging


# In[2]:


# Set the logging level to a higher level to reduce verbosity
logging.getLogger().setLevel(logging.WARNING)


# In[7]:


# Set the log level to WARN
os.environ['SPARK_LOG_LEVEL'] = 'WARN'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.memory=2g --conf spark.executor.memory=2g pyspark-shell'


# In[8]:


# # Set log level to ERROR for Spark-related logs
# logger = SparkSession.builder.getOrCreate()._jvm.org.apache.log4j
# logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)


# In[9]:


spark = SparkSession.builder \
    .appName("DeltaLakeToNeo4j") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.jars", "/neo4j-connector/neo4j-connector-apache-spark_2.12_3.0-4.0.0.jar") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.neo4j.bolt.url", "neo4j://10.4.41.56:7687") \
    .config("spark.neo4j.bolt.user", "neo4j") \
    .config("spark.neo4j.bolt.password", "foodie")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# In[10]:


driver = GraphDatabase.driver("neo4j://10.4.41.56:7687", auth=("neo4j", "foodie"))
session = driver.session()


# In[11]:


# Create a session to run queries
with driver.session() as session:
    # Run the query
    query = """
        MATCH (dish:Dishes {id: 8})
        RETURN dish
    """
    result = session.run(query)
    
    # Print the result
    for record in result:
        dish = record["dish"]
        print(dish)


# In[12]:


# Open a session
with driver.session() as session:
    # Execute a query to retrieve the list of databases
    result = session.run("SHOW DATABASES")

    # Print the names of the databases
    for record in result:
        database_name = record["name"]
        print(database_name)

# Close the driver
driver.close()


# In[14]:


# # UNCOMMENT IF YOU WANT TO RELOAD ALL NODES
# batch_size = 1000

# # Delete nodes in batches
# with driver.session() as session:
#     while True:
#         # Start a new transaction for each batch
#         with session.begin_transaction() as tx:
#             # Match and delete nodes in the current batch
#             result = tx.run("""
#                 MATCH (n)
#                 WITH n LIMIT $batchSize
#                 DETACH DELETE n
#                 RETURN count(n)
#                 """, batchSize=batch_size)
#             deleted_count = result.single()[0]

#             # If no nodes were deleted, we have deleted all nodes
#             if deleted_count == 0:
#                 break

# # Close the Neo4j driver
# driver.close()


# ### Creating the nodes

# In[16]:


# Load the Delta Lake table into a DataFrame
df_michelin = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/michelin")

# Create a temporary view for the DataFrame
df_michelin.createOrReplaceTempView("michelin")

with driver.session() as session:
    # Define the Cypher query to create nodes
    for row in df_michelin.collect():
        # Create a node for each row
        session.run(
            "MERGE (m:Michelin {ra_key: $ra_key})"
            "SET m.number_of_stars = $number_of_stars, m.year = $year",
            ra_key=row.ra_key,
            number_of_stars=row.number_of_stars,
            year=row.year
        )

driver.close()


# In[29]:


# Load the Delta Lake table into a DataFrame
df_restaurant = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")

# Create a temporary view for the DataFrame
df_restaurant.createOrReplaceTempView("restaurant")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_restaurant.collect():
        # Create a node for each row
        session.run(
            "MERGE (r:Restaurant {key: $key})"
            "SET r.restaurant_name = $restaurant_name, r.address = $address, r.latitude = $latitude, \
            r.longitude = $longitude, r.website = $website, r.email = $email, r.telephone = $telephone, \
            r.ta_key = $ta_key, r.go_key = $go_key, r.closed = $closed",
            key=row.key,
            restaurant_name=row.restaurant_name,
            address=row.address,
            latitude=row.latitude,
            longitude=row.longitude,
            website=row.website,
            email=row.email,
            telephone=row.telephone,
            ta_key=row.ta_key,
            go_key=row.go_key,
            closed=row.closed
        )

driver.close()


# In[20]:


# Load the Delta Lake table into a DataFrame
df_images = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images")

# Create a temporary view for the DataFrame
df_images.createOrReplaceTempView("images")

with driver.session() as session:
    for row in df_images.collect():
        session.run(
            "MERGE (r:Images {id: $id}) "
            "SET r.file_url = $file_url, r.dishId = $dishId, r.uploaderId = $uploaderId, r.uploadDt = $uploadDt",
            id=row.id,
            file_url=row.file_url,
            dishId=row.dishId,
            uploaderId=row.uploaderId,
            uploadDt=row.uploadDt
        )
driver.close()


# In[21]:


# Load the Delta Lake table into a DataFrame
df_images_ex = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images_external")

# Create a temporary view for the DataFrame
df_images_ex.createOrReplaceTempView("images")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_images_ex.collect():
        # Create a node for each row
        session.run(
            "MERGE (i:Images_ex {image_id: $image_id})"
            "SET i.ra_key = $ra_key, i.image_file = $image_file, i.source = $source",
            image_id=row.image_id,
            ra_key=row.ra_key,
            image_file=row.image_file,
            source=row.source

        )
driver.close()


# In[22]:


# Load the Delta Lake table into a DataFrame
df_cuisines = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/cuisines")

# Create a temporary view for the DataFrame
df_cuisines.createOrReplaceTempView("cuisines")

# Define the Cypher query to create nodes
with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_cuisines.collect():
        # Create a node for each row
        session.run(
            "MERGE (c:Cuisines {cuisine_key: $cuisine_key})"
            "SET c.restaurant_key = $restaurant_key, c.cuisine = $cuisine",
            cuisine_key=row.cuisine_key,
            restaurant_key=row.restaurant_key,
            cuisine=row.cuisine
        )
driver.close()


# In[ ]:


# Load the Delta Lake table into a DataFrame
df_long_review = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/long_review")

# Create a temporary view for the DataFrame
df_long_review.createOrReplaceTempView("review_long")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_long_review.collect():
        # Create a node for each row
        session.run(
            "MERGE (r:Review {review_key: $review_key})"
            "SET r.time = $time, r.rating = $rating, r.ra_key = $ra_key, r.sample = $sample, r.text = $text, r.source = $source",
            review_key=row.review_key,
            time=row.time,
            rating=row.rating,
            ra_key=row.ra_key,
            sample=row.sample,
            text=row.text,
            source=row.source
        )

# Close the Neo4j driver
driver.close()


# In[ ]:


# Load the Delta Lake table into a DataFrame
df_review = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/reviews")

# Create a temporary view for the DataFrame
df_review.createOrReplaceTempView("reviews")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_review.collect():
        # Create a node for each row
        session.run(
            "MERGE (r:Review {id: $id})"
            "SET r.userId = $userId, r.dishId = $dishId, r.rating = $rating, r.timestamp = $timestamp, r.text = $text",
            id=row.id,
            userId=row.userId,
            dishId=row.dishId,
            rating=row.rating,
            timestamp=row.timestamp,
            text=row.text
        )

# Close the Neo4j driver
driver.close()


# In[ ]:


# Load the Delta Lake table into a DataFrame
df_user = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/users")

# Create a temporary view for the DataFrame
df_user.createOrReplaceTempView("users")

# Define the Cypher query to create nodes
with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_user.collect():
        # Create a node for each row
        session.run(
            "MERGE (u:User {id: $id})"
            "SET u.sex = $sex, u.createDate = $createDate, u.nationality = $nationality, u.occupation = $occupation",
            id=row.id,
            sex=row.sex,
            createDate=row.createDate,
            nationality=row.nationality,
            occupation=row.occupation
        )
driver.close()


# In[13]:


# Load the Delta Lake table into a DataFrame
df_weekday = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/weekday")

# Create a temporary view for the DataFrame
df_weekday.createOrReplaceTempView("weekday")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_weekday.collect():
        # Create a node for each row
        session.run(
            "MERGE (w:Weekday {day_key: $day_key})"
            "SET w.weekday = $weekday",
            day_key=row.day_key,
            weekday=row.weekday
        )

# Close the Neo4j driver
driver.close()


# In[14]:


# Load the Delta Lake table into a DataFrame
df_dishes = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/dishes")

# Create a temporary view for the DataFrame
df_dishes.createOrReplaceTempView("dishes")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_dishes.collect():
        # Create a node for each row
        session.run(
            "MERGE (d:Dishes {id: $id})"
            "SET d.name = $name, d.description = $description, d.restaurantId = $restaurantId",
            id=row.id,
            name=row.name,
            description=row.description,
            restaurantId=row.restaurantId
        )
driver.close()


# In[15]:


# Load the Delta Lake table into a DataFrame
df_ingredients = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/ingredients")

# Create a temporary view for the DataFrame
df_ingredients.createOrReplaceTempView("ingredients")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_ingredients.collect():
        # Create a node for each row
        session.run(
            "MERGE (d:Ingredients{id: $id})"
            "SET d.ingredient = $ingredient",
             id=row.id,
             ingredient=row.ingredient
           
        )
driver.close()


# In[17]:


# Load the Delta Lake table into a DataFrame
df_tags = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/tags")

# Create a temporary view for the DataFrame
df_tags.createOrReplaceTempView("tags")

with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_tags.collect():
        # Create a node for each row
        session.run(
            "MERGE (d:Tags {id: $id})"
            "SET d.tag = $tag",
            id=row.id,
            tag=row.tag
        )

# Close the Neo4j driver
driver.close()


# In[18]:


# Load the Delta Lake table into a DataFrame
df_opening_hours = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/opening_hours")

# Create a temporary view for the DataFrame
df_opening_hours.createOrReplaceTempView("opening_hours")

# Define the Cypher query to create nodes
with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_opening_hours.collect():
        # Create a node for each row
        session.run(
            "MERGE (o:Opening_hours {ra_key: $ra_key, day_key: $day_key})"
            "SET o.open_time = $open_time, o.close_time = $close_time",
            ra_key=row.ra_key,
            day_key=row.day_key,
            open_time=row.open_time,
            close_time=row.close_time
            

        )
driver.close()


# In[18]:


# Load the Delta Lake table into a DataFrame
df_interestedCuisines = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/interestedCuisines")

# Create a temporary view for the DataFrame
df_interestedCuisines.createOrReplaceTempView("interestedCuisines")

# Define the Cypher query to create nodes
with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_interestedCuisines.collect():
        # Create a node for each row
           session.run(
            "MERGE (c:interestedCuisines {id: $id})"
            "SET c.interestedCuisine = $interestedCuisine",
            id=row.id,
            interestedCuisine=row.interestedCuisine
        )
driver.close()


# In[37]:


# Load the Delta Lake table into a DataFrame
df_meals = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/meals")

# Create a temporary view for the DataFrame
df_meals.createOrReplaceTempView("meals")

# Define the Cypher query to create nodes
with driver.session() as session:
    # Iterate over the rows of the DataFrame
    for row in df_meals.collect():
        # Create a node for each row
           session.run(
            "MERGE (c:Meals {meal_key: $meal_key})"
            "SET c.restaurant_key=$restaurant_key, c.meals = $meals",
            meal_key=row.meal_key,
            restaurant_key=row.restaurant_key,
            meals=row.meals
        )
driver.close()


# ### Creating relationships

# In[21]:


# Read DataFrames
user_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/users")
user_cuisine_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/user_cuisines")
cuisine_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/interestedCuisines")

# Register the DataFrames as temporary views
user_df.createOrReplaceTempView("users")
user_cuisine_df.createOrReplaceTempView("user_cuisines")
cuisine_df.createOrReplaceTempView("interestedCuisines")

# Define the Cypher query
query = """
MERGE (u:User {id: $user_id})
MERGE (c:interestedCuisines {id: $cuisine_id})
CREATE (u)-[:INTERESTED_IN]->(c)
"""

# Execute the query for each row in the intermediate table
with driver.session() as session:
    for row in user_cuisine_df.collect():
        user_id = row['userId']
        cuisine_id = row['interestedCuisineId']
        session.run(query, user_id=user_id, cuisine_id=cuisine_id)

# Close the Neo4j driver
driver.close()


# In[22]:


# Read Spark DataFrames
user_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/users")
image_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images")

# Register the DataFrames as temporary views
user_df.createOrReplaceTempView("users")
image_df.createOrReplaceTempView("images")

# Define the Cypher query
query = """
    MERGE (u:User {id: $uploader_id})
    MERGE (i:Images {id: $imageId})
    CREATE (u)-[:UPLOADED]->(i)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for image_row in image_df.collect():
        uploader_id = image_row['uploaderId']
        imageId = image_row['id']
        session.run(query, uploader_id=uploader_id, imageId=imageId)

# Close the Neo4j driver
driver.close()


# In[23]:


# Read Spark DataFrames
user_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/users")
locations_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/locations")

# Register the DataFrames as temporary views
user_df.createOrReplaceTempView("users")
locations_df.createOrReplaceTempView("locations")

# Define the Cypher query
query = """
    MERGE (u:User {id: $userId})
    MERGE (i:Locations {id: $id})
    CREATE (u)-[:IS_LOCATED]->(i)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for locations_row in locations_df.collect():
        userId = locations_row['userId']
        id = locations_row['id']
        session.run(query, id=id, userId=userId)

# Close the Neo4j driver
driver.close()


# In[30]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
image_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
image_df.createOrReplaceTempView("images")

# Define the Cypher query
query = """
    MATCH (r:Restaurant {id: $key})
    MERGE (i:Images {id: $imageId})
    CREATE (r)-[:HAS_IMAGE]->(i)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for image_row in image_df.collect():
        key = image_row['restaurantId']
        imageId = image_row['id']
        session.run(query, key=key, imageId=imageId)

# Close the Neo4j driver
driver.close()


# In[35]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
images_external_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images_external")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
images_external_df.createOrReplaceTempView("images_external")

# Define the Cypher query
query = """
    MATCH (r:Restaurant {key: $ra_key})
    MERGE (i:Images_ex {image_id: $image_id})
    CREATE (i)-[:EX_BELONGS_TO]->(r)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for images_external_row in images_external_df.collect():
        ra_key = images_external_row['ra_key']
        image_id = images_external_row['image_id']
        session.run(query, ra_key=ra_key, image_id=image_id)

# Close the Neo4j driver
driver.close()


# In[36]:


# Read DataFrames
image_tags_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/image_tags")
tag_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/tags")
images_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images")

# Register the DataFrames as temporary views
image_tags_df.createOrReplaceTempView("image_tags")
tag_df.createOrReplaceTempView("tags")
images_df.createOrReplaceTempView("images")

# Define the Cypher query
query = """
MERGE (u:Images {id: $imageId})
MERGE (c:Tags {id: $tagId})
CREATE (u)-[:HAS_TAG]->(c)
"""

# Execute the query for each row in the intermediate table
with driver.session() as session:
    for row in image_tags_df.collect():
        imageId = row['imageId']
        tagId = row['tagId']
        session.run(query, imageId=imageId, tagId=tagId)

# Close the Neo4j driver
driver.close()


# In[37]:


# Read the user and image data into Spark DataFrames
user_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/users")
reviews_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/reviews")

# Register the DataFrames as temporary views
user_df.createOrReplaceTempView("users")
reviews_df.createOrReplaceTempView("images")

# Define the Cypher query
query = """
    MERGE (u:User {id: $userId})
    MERGE (i:Review {id: $id})
    CREATE (u)-[:WROTE]->(i)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for reviews_row in reviews_df.collect():
        userId = reviews_row['userId']
        id = reviews_row['id']
        session.run(query, userId=userId, id=id)

# Close the Neo4j driver
driver.close()


# In[13]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
dish_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/dishes")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
dish_df.createOrReplaceTempView("dishes")

# Define the Cypher query
query = """
    MERGE (r:Dishes {id: $id})
    MERGE (i:Restaurant {key: $restaurantId})
    CREATE (r)-[:ON_MENU]->(i)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for dish_row in dish_df.collect():
        restaurantId = dish_row['restaurantId']
        id = dish_row['id']
        session.run(query, restaurantId=restaurantId, id=id)

# Close the Neo4j driver
driver.close()


# In[15]:


# Read Spark DataFrames
ingredients_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/ingredients")
dish_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/dishes")
ingredients_in_dish_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/ingredients_in_dish")

# Register the DataFrames as temporary views
ingredients_df.createOrReplaceTempView("restaurants")
dish_df.createOrReplaceTempView("dishes")
ingredients_in_dish_df.createOrReplaceTempView("ingredients_in_dish")

# Define the Cypher query
query = """
MERGE (u:Dishes {id: $dishId})
MERGE (c:Ingredients {id: $ingredientId})
CREATE (u)-[:MADE_OF]->(c)
"""

# Execute the query for each row in the intermediate table
with driver.session() as session:
    for row in ingredients_in_dish_df.collect():
        dishId = row['dishId']
        ingredientId = row['ingredientId']
        session.run(query, dishId=dishId, ingredientId=ingredientId)

# Close the Neo4j driver
driver.close()


# In[16]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
cuisines_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/cuisines")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
cuisines_df.createOrReplaceTempView("cuisines")

# Define the Cypher query
query = """
    MERGE (r:Restaurant {key: $restaurant_key})
    MERGE (c:Cuisines {id: $cuisine_key})
    CREATE (r)-[:HAS_CUISINES]->(c)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for cuisines_row in cuisines_df.collect():
        restaurant_key = cuisines_row['restaurant_key']
        cuisine_key = cuisines_row['cuisine_key']
        session.run(query, restaurant_key=restaurant_key, cuisine_key=cuisine_key)

# Close the Neo4j driver
driver.close()


# In[43]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
meals_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/meals")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
meals_df.createOrReplaceTempView("meals")

# Define the Cypher query
query = """
    MERGE (r:Restaurant {key: $restaurant_key})
    WITH r
    MATCH (c:Meals {meal_key: $meal_key})
    CREATE (r)-[:OFFERS_MEALS]->(c)
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for meals_row in meals_df.collect():
        restaurant_key = meals_row['restaurant_key']
        meal_key = meals_row['meal_key']
        session.run(query, restaurant_key=restaurant_key, meal_key=meal_key)

# Close the Neo4j driver
driver.close()



# In[23]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
opening_hours_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/opening_hours")
weekday_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/weekday")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
opening_hours_df.createOrReplaceTempView("opening_hours")
weekday_df.createOrReplaceTempView("weekday")

# Define the Cypher query
query = """
     MATCH (r:Restaurant {key: $ra_key})
    MATCH (w:Weekday {day_key: $day_key})
    MERGE (r)-[o:OPENING_HOURS]->(w)
    SET o.open_time = $open_time, o.close_time = $close_time
        """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for opening_hours_row in opening_hours_df.collect():
        ra_key = opening_hours_row['ra_key']
        day_key = opening_hours_row['day_key']
        open_time=opening_hours_row['open_time']
        close_time=opening_hours_row['close_time']
        session.run(query, ra_key=ra_key, day_key=day_key, open_time=open_time,close_time=close_time)

# Close the Neo4j driver
driver.close()


# In[21]:


# Read Spark DataFrames
restaurants_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/restaurants")
michelin_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/michelin")

# Register the DataFrames as temporary views
restaurants_df.createOrReplaceTempView("restaurants")
michelin_df.createOrReplaceTempView("michelin")

# Define the Cypher query
query = """
    MATCH (r1:Restaurant {key: $ra_key1})
    MATCH (r2:Restaurant {key: $ra_key2})
    MERGE (r1)-[h:HAS_MICHELIN]->(r2)
    SET h.number_of_stars = $number_of_stars, h.year = $year
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for michelin_row in michelin_df.collect():
        ra_key1 = michelin_row['ra_key']
        ra_key2 = michelin_row['ra_key']
        number_of_stars = michelin_row['number_of_stars']
        year = michelin_row['year']
        session.run(query, ra_key1=ra_key1, ra_key2=ra_key2, number_of_stars=number_of_stars, year=year)

# Close the Neo4j driver
driver.close()


# In[ ]:


# Read Spark DataFrames
user_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/users")
image_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/images")
swipes_df = spark.read.format("delta").load("hdfs://localhost:9000/user/hadoop/delta/warehouse/swipes")

# Register the DataFrames as temporary views
user_df.createOrReplaceTempView("users")
image_df.createOrReplaceTempView("image")
swipes_df.createOrReplaceTempView("swipes")

# Define the Cypher query
query = """
    MERGE (u:User {id: $userId})
    MERGE (i:Images {id: $imageId})
    CREATE (u)-[s:SWIPED]->(i)
    SET s.timestamp = $timestamp, s.swipeLeft = $swipeLeft
    """

# Execute the query for each row in the DataFrames
with driver.session() as session:
    for swipes_row in swipes_df.collect():
        userId = swipes_row['userId']
        imageId = swipes_row['imageId']
        timestamp = swipes_row['timestamp']
        swipeLeft = swipes_row['swipeLeft']
        session.run(query, userId=userId, imageId=imageId, timestamp=timestamp, swipeLeft=swipeLeft)

# Close the Neo4j driver
driver.close()


# In[ ]:


# Close the SparkSession
spark.stop()

