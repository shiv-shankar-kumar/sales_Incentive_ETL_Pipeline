# Databricks notebook source
# MAGIC %md
# MAGIC PySpark Code to Connect MySQL

# COMMAND ----------

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("MySQL_JDBC_Connection") \
    .getOrCreate()

# Use ngrok TCP host and port
database_host = "0.tcp.ngrok.io"
database_port = "14324"  # Update with your actual forwarded port from ngrok
database_name = "youtube_project"
user = "root"
password = "258634"

# JDBC URL
jdbc_url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=false&serverTimezone=UTC"

# Table to read from
table_name = "product_staging_table"

# Read data
try:
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()
    
    print("✅ Data loaded successfully:")
    df.show()
except Exception as e:
    print("❌ Error reading data from MySQL:")
    print(e)


# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

# Create Spark session
# spark = SparkSession.builder \
#     .appName("GenerateAndSaveCSV") \
#     .getOrCreate()

# Step 1: Generate random dataset using pandas
np.random.seed(42)  # For reproducibility
data = {
    "id": np.arange(1, 101),
    "name": [f"Item_{i}" for i in range(1, 101)],
    "price": np.random.uniform(10.0, 500.0, 100).round(2),
    "category": np.random.choice(["A", "B", "C"], 100)
}

pdf = pd.DataFrame(data)

# Step 2: Convert pandas DataFrame to PySpark DataFrame
df = spark.createDataFrame(pdf)

df.count()

# COMMAND ----------


# Step 3: Save the DataFrame as CSV in DBFS
df.write.mode("overwrite").option("header", True).csv("/tmp/random_products")

print("CSV saved successfully to DBFS: /tmp/random_products")

# COMMAND ----------

# dbutils.fs.ls(:Filestore//tables)

dbutils.fs.ls("/tmp/random_products")


# COMMAND ----------

files = dbutils.fs.ls("/FileStore/tables")
print(f"Number of files: {len(files)}")


# COMMAND ----------

# Delete old multi-part output
dbutils.fs.rm("/tmp/random_products", True)

# Write single CSV file to FileStore/tables
df.coalesce(1).write.csv("/FileStore/tables/random_products_single_file", header=True, mode="overwrite")


# COMMAND ----------

2+2

# COMMAND ----------

# database_host = "0.tcp.ngrok.io"
# database_port = "14324"  # Update with your actual forwarded port from ngrok
# database_name = "youtube_project"
# user = "root"
# password = "258634"


def load_mysql_table_as_df(table_name):
    mysql_url = "jdbc:mysql://0.tcp.ngrok.io:14324/youtube_project"
    
    mysql_properties = {
        "user": "root",
        "password": "258634",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    try:
        df = spark.read.jdbc(url=mysql_url, table=table_name, properties=mysql_properties)
        print(f"✅ Table '{table_name}' loaded successfully.")
        return df
    except Exception as e:
        print(f"❌ Failed to load table '{table_name}': {e}")
        return None


# COMMAND ----------

product_df = load_mysql_table_as_df("product")
product_df.show()


# COMMAND ----------

customer_df = load_mysql_table_as_df("customer")
customer_df.show(5)

# COMMAND ----------

sales_team_df = load_mysql_table_as_df("sales_team")
sales_team_df.count()
sales_team_df.show(5)

# COMMAND ----------

store_df = load_mysql_table_as_df("store")
store_df.count()


# COMMAND ----------

store_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###now we've all dimension table, and we'll create a fact table(which will come directly from s3 named sales_data.csv)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import random
from datetime import datetime, timedelta

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Sample data
customer_ids = list(range(1, 21))
store_ids = list(range(121, 124))
product_data = {
    "quaker oats": 212.0,
    "sugar": 50.0,
    "maida": 20.0,
    "besan": 52.0,
    "refined oil": 110.0,
    "clinic plus": 1.5,
    "dantkanti": 100.0,
    "nutrella": 40.0
}
sales_persons = {
    121: [1, 2, 3],
    122: [4, 5, 6],
    123: [7, 8, 9]
}

start_date = datetime(2024, 3, 3)
end_date = datetime(2024, 8, 20)

# Create list of dicts (not Row)
records = []
for _ in range(50):
    customer_id = random.choice(customer_ids)
    store_id = random.choice(store_ids)
    product_name = random.choice(list(product_data.keys()))
    sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    sales_person_id = random.choice(sales_persons[store_id])
    quantity = random.randint(1, 10)
    price = product_data[product_name]
    total_cost = price * quantity

    records.append({
        "customer_id": customer_id,
        "store_id": store_id,
        "product_name": product_name,
        "sales_date": sales_date.strftime("%Y-%m-%d"),
        "sales_person_id": sales_person_id,
        "price": price,
        "quantity": quantity,
        "total_cost": total_cost
    })

# Explicit schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", StringType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", DoubleType(), True)
])

# Create DataFrame
sales_df = spark.createDataFrame(records, schema=schema)

# Save as a single CSV file in DBFS
output_path = "/FileStore/tables/sales_data.csv"
sales_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print("✅ CSV saved at:", output_path)


# COMMAND ----------

sales_df.show(5)

# COMMAND ----------

sales_df = spark.read.option("header", True).option("inferSchema", True).csv("/FileStore/tables/sales_data.csv")

# Show the DataFrame
sales_df.show(5)


# COMMAND ----------

final_df_to_process = sales_df

# COMMAND ----------

final_df_to_process.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Join 1 (sales table with cusotmer)

# COMMAND ----------

# Perform an inner join on customer_id
s3_customer_to_join = final_df_to_process.join(customer_df, on="customer_id", how="inner")

# Optional: Display the joined DataFrame
s3_customer_to_join.show(5)


# COMMAND ----------

# Drop specified columns
s3_customer_to_join = s3_customer_to_join.drop("product_name", "price", "quantity", "customer_joining_date")

# Optional: Show the cleaned DataFrame
s3_customer_to_join.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC #join 2 : last final table with store 

# COMMAND ----------

# # Perform inner join on store_id and id
# s3_customer_store_df_join = s3_customer_to_join.join(
#     store_df,
#     s3_customer_to_join.store_id == store_df.id,
#     "inner"
# )
# # Optional: Show the joined result
# s3_customer_store_df_join.display(5)

# Perform the join with disambiguated column references
s3_customer_store_df_join = s3_customer_to_join.join(
    store_df,
    s3_customer_to_join.store_id == store_df.id,
    "inner"
).select(
    # Columns from s3_customer_to_join
    s3_customer_to_join.customer_id,
    s3_customer_to_join.store_id,
    s3_customer_to_join.sales_date,
    s3_customer_to_join.sales_person_id,
    s3_customer_to_join.total_cost,
    s3_customer_to_join.first_name,
    s3_customer_to_join.last_name,
    s3_customer_to_join.address.alias("customer_address"),  # Disambiguated
    s3_customer_to_join.pincode,
    s3_customer_to_join.phone_number,

    # Columns from store_df (renamed to avoid conflicts)
    store_df.address.alias("store_address"),
    store_df.store_pincode,
    store_df.store_manager_name,
    store_df.store_opening_date,
    store_df.reviews
)



# COMMAND ----------

s3_customer_store_df_join = s3_customer_store_df_join.drop( "customer_address", "store_pincode","store_opening_date", "reviews")
# s3_customer_store_df_join.show(5)

s3_customer_store_df_join.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #3rd join last final table with sales_team

# COMMAND ----------

  sales_team_df.show(5)

# COMMAND ----------

# Perform inner join on sales_person_id and id
# s3_customer_store_sales_df_join = s3_customer_store_df_join.join(
#     sales_team_df,
#     s3_customer_store_df_join.sales_person_id == sales_team_df.id,
#     "inner"
# )
# # Optional: Show the final joined result
# s3_customer_store_sales_df_join.drop()

# Perform the join with disambiguated column names
s3_customer_store_sales_df_join = s3_customer_store_df_join.join(
    sales_team_df,
    s3_customer_store_df_join.sales_person_id == sales_team_df.id,
    "inner"
).select(
    # Columns from s3_customer_store_df_join
    s3_customer_store_df_join.customer_id,
    s3_customer_store_df_join.store_id,
    s3_customer_store_df_join.sales_date,
    s3_customer_store_df_join.sales_person_id,
    s3_customer_store_df_join.total_cost,
    s3_customer_store_df_join.first_name.alias("customer_first_name"),
    s3_customer_store_df_join.last_name.alias("customer_last_name"),
    s3_customer_store_df_join.pincode.alias("customer_pincode"),
    s3_customer_store_df_join.phone_number,
    s3_customer_store_df_join.store_address,
    s3_customer_store_df_join.store_manager_name,

    # Columns from sales_team_df (renamed to avoid conflicts)
    sales_team_df.first_name.alias("sales_first_name"),
    sales_team_df.last_name.alias("sales_last_name"),
    sales_team_df.address.alias("sales_address"),
    sales_team_df.pincode.alias("sales_pincode"),
    sales_team_df.manager_id,
    sales_team_df.is_manager,
    sales_team_df.joining_date
)


# COMMAND ----------

s3_customer_store_sales_df_join = s3_customer_store_sales_df_join.drop()

s3_customer_store_sales_df_join.display(5)

# COMMAND ----------

# s3_customer_store_sales_df_join_final = s3_customer_store_sales_df_join.drop("first_name","last_name","pincode","address" )

# s3_customer_store_sales_df_join_final.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###now we'll write the customer data into customer datamart in parquet format.
# MAGIC

# COMMAND ----------

final_customer_data_mart_df = s3_customer_store_sales_df_join.select("customer_id","customer_first_name","customer_last_name", "store_address","customer_pincode", "phone_number", "sales_date", "total_cost" )

final_customer_data_mart_df.show(5)

# COMMAND ----------


final_customer_data_mart_df.write.mode("overwrite").parquet("/FileStore/tables/custoerm_data_mart")
# final_customer_data_mart_df.write.mode("overwrite").parquet("file:///D:/youtube_project/customer_data_mart")



# COMMAND ----------

import os

# Define the DBFS path where you want to save the Parquet file
dbfs_path = "/dbfs/tmp/customer_data_mart"

# Write the DataFrame to DBFS in Parquet format, overwriting any existing data
final_customer_data_mart_df.write.mode("overwrite").parquet(dbfs_path)

print(f"Data successfully written to DBFS at {dbfs_path}")




# COMMAND ----------

# dbutils.fs.ls("/FileStore/tables")    

# Define the DBFS path where you want to save the Parquet file
# dbfs_path = "/FileStore/tables/final_customer_data_mart_df.parquet"
dbfs_path = "/FileStore/tables/customer_data_mart_df.csv"
# /FileStore/tables/sales_data.csv

# Write the DataFrame to DBFS in Parquet format, overwriting any existing data
final_customer_data_mart_df.write.mode("overwrite").parquet(dbfs_path)

print(f"Data successfully written to DBFS at {dbfs_path}")


# COMMAND ----------

# MAGIC %md 
# MAGIC ###now we'll create the sales data mart and upload the sales team data into it

# COMMAND ----------

s3_customer_store_sales_df_join = s3_customer_store_sales_df_join.withColumn(
    "sales_month", date_format("sales_date", "yyyy-MM")
)

final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select("store_id", "sales_person_id","sales_first_name", "sales_last_name","store_manager_name", "manager_id", "is_manager", "sales_address","sales_pincode", "sales_date", "total_cost", "sales_month")

final_sales_team_data_mart_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###great, you are hitting the project 
# MAGIC
# MAGIC now we'll write our data in to partition so that it'll help us to do optimization. 
# MAGIC
# MAGIC example : under aadhar records if anyone to find my aadhar number. so it'll check into Bihar state, and Bihariganj city.

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/"))


# COMMAND ----------

final_sales_team_data_mart_df.write \
    .mode("overwrite") \
    .partitionBy("sales_month", "store_id") \
    .parquet("/FileStore/tables/sales_data_mart")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##this command will omit all of your files from dbfs : be cautions
# MAGIC

# COMMAND ----------

# dbutils.fs.rm("/FileStore/tables/", recurse=True)


# COMMAND ----------


df = spark.read.format('parquet').option("inferSchema", True).load("/FileStore/tables/sales_data_mart")


# COMMAND ----------

df.display()

# COMMAND ----------

df1 = spark.read.format('parquet').option('inferSchema', True).load("file:///D:/youtube_project/customer_data_mart")

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##now we'll write the aggregated customer data into the database mysql
# MAGIC
# MAGIC customer ki sale btayega on monthly basis

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window = Window.partitionBy("customer_id", "sales_date_month")

# Transform the data
final_customer_data_mart = final_customer_data_mart_df \
    .withColumn("sales_date_month", substring(col("sales_date"), 1, 7)) \
    .withColumn("total_sales_every_month_by_each_customer", sum("total_cost").over(window)) \
    .select(
        "customer_id",
        concat(col("customer_first_name"), lit(" "), col("customer_last_name")).alias("full_name"),
        "store_address",
        "phone_number",
        "sales_date_month",
        col("total_sales_every_month_by_each_customer").alias("total_sales")
    ) \
    .distinct()

# COMMAND ----------

final_customer_data_mart.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##we'll try to upload the same data into mysql database : youtube_project database
# MAGIC

# COMMAND ----------

# Use ngrok TCP host and port
database_host = "0.tcp.ngrok.io"
database_port = "14324"  # Update with your actual forwarded port from ngrok
database_name = "youtube_project"
user = "root"
password = "258634"

# JDBC URL
jdbc_url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=false&serverTimezone=UTC"


final_customer_data_mart.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "customer_data_mart_table") \
    .option("user", "root") \
    .option("password", "258634") \
    .mode("overwrite") \
    .save()


# COMMAND ----------

# MAGIC %md
# MAGIC great bro, this is done.

# COMMAND ----------

from pyspark.sql import functions as F


# Step 1: Extract "sales_month" from sales_date
sales_df = final_sales_team_data_mart_df.withColumn(
    "sales_month", F.substring(F.col("sales_date"), 1, 7)
)

# Step 2: Compute total sales per store_id, sales_person_id, and sales_month
window_spec = Window.partitionBy("store_id", "sales_person_id", "sales_month")
sales_df = sales_df.withColumn(
    "total_sales_every_month", F.sum("total_cost").over(window_spec)
).distinct()

# Step 3: Rank sales people within each store per month
rank_window = Window.partitionBy("store_id", "sales_month").orderBy(F.col("total_sales_every_month").desc())

sales_df = sales_df.withColumn("rnk", F.rank().over(rank_window))

# Step 4: Calculate 1% incentive only for rank 1
sales_df = sales_df.withColumn(
    "incentive",
    F.when(F.col("rnk") == 1, F.col("total_sales_every_month") * 0.01).otherwise(F.lit(0))
).withColumn("incentive", F.round(F.col("incentive"), 2))

# Step 5: Concatenate full name and select final columns
sales_df = sales_df.withColumn(
    "full_name", F.concat_ws(" ", F.col("sales_first_name"), F.col("sales_last_name"))
)

final_sales_team_data_mart_table = sales_df.select(
    "store_id",
    "sales_person_id",
    "full_name",
    "sales_month",
    F.col("total_sales_every_month").alias("total_sales"),
    "incentive"
).distinct()


# COMMAND ----------

final_sales_team_data_mart_table.display()


# COMMAND ----------

# MAGIC %md
# MAGIC