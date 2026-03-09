#!/usr/bin/env python
# coding: utf-8

# ## Medallion Architecture Implementation
# 
# New notebook

# ## Medallion Architecture Implementation
# ### Links and Resources
# - https://learn.microsoft.com/en-us/training/modules/describe-medallion-architecture/2-describe-medallion-architecture
# - https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture

# ### Utilities

# In[10]:


from pyspark.sql.functions import col, current_timestamp, date_format, round, sum


# ### Bronze Layer Processing

# ##### Reading in the retail dataset files to dataframes

# In[11]:


# orders

# Set the path to the orders data
path_orders = "Files/retail_dataset_de/orders"

# Read the orders data from a Parquet file
# Add a new column for the current timestamp
# Save the data into the 'bronze_orders' table, adding to it if it already exists
spark.\
        read.\
        format("parquet").\
        load(path_orders).\
        withColumn("date_processed", current_timestamp()).\
        write.\
        mode("append").\
        saveAsTable('bronze_orders')


# In[12]:


# products

# Set the path to the products data
path_products = "Files/retail_dataset_de/products.parquet"

# Read the products data from a Parquet file
# Add a new column for the current timestamp
# Save the data into the 'bronze_products' table, adding to it if it already exists
spark.\
        read.\
        format("parquet").\
        load(path_products).\
        withColumn("date_processed", current_timestamp()).\
        write.\
        mode("append").\
        saveAsTable('bronze_products')


# In[15]:


# reviews

# Set the path to the reviews data
path_reviews = "Files/retail_dataset_de/reviews.json"

# Define the schema for the JSON reviews data
schema_json = "id INT, created_at TIMESTAMP, reviewer STRING, product_id INT, rating DECIMAL(5,2), body STRING"

# Read the reviews data using the schema
# Add a new column for the current timestamp
# Save the data into the 'bronze_reviews' table, appending new data if the table exists
spark.\
        read.\
        schema(schema_json).\
        format("json").\
        load(path_reviews).\
        withColumn("date_processed", current_timestamp()).\
        write.\
        mode("append").\
        saveAsTable('bronze_reviews')


# In[18]:


#users

# Set the path to the users data
path_users = "Files/retail_dataset_de/users.csv"

# Define the schema for the users CSV data
schema_users = "id INT, created_at TIMESTAMP, name STRING, email STRING, city STRING, state STRING, zip STRING, birth_date DATE, source STRING"

# Read the users data from a CSV file using the specified schema
# Include the header in the CSV file
# Add a new column for the current timestamp
# Save the data into the 'bronze_users' table, appending new data if the table exists
spark.\
      read.\
      option("header", "true").\
      schema(schema_users).\
      format("csv").\
      load(path_users).\
      withColumn("date_processed", current_timestamp()).\
      write.\
      mode("append").\
      saveAsTable('bronze_users')


# ### Silver Layer Processing

# In[19]:


# orders

# Read data from the 'bronze_orders' table
# Calculate the order total and format the order date
# Select specific columns and write the result to the 'silver_orders' table in append mode

spark.\
        read.\
        table("bronze_orders").\
        withColumn("order_total", round(col("quantity")*col("unit_price"),2)).\
        withColumn("order_date", date_format(col("created_at"), "yyyy-MM-dd").cast("date")).\
        select(
                col("id"), 
                col("order_date"), 
                col("user_id"), 
                col("product_id"), 
                col("quantity"),
                col("unit_price"),
                col("order_total"),
                col("date_processed")
                ).\
        write.\
        mode("append").\
        saveAsTable("silver_orders")


# In[20]:


# products

# Read data from the 'bronze_products' table
# Select specific columns, renaming some of them for clarity
# Write the result to the 'silver_products' table in append mode

spark.\
        read.\
        table("bronze_products").\
        select(
                col("id").alias("product_id"),
                col("created_at"), 
                col("title").alias("product_name"), 
                col("category").alias("product_category"), 
                col("ean"),
                col("vendor"),
                col("date_processed")
                ).\
        write.\
        mode("append").\
        saveAsTable("silver_products")


# ### Gold Layer Processing

# In[21]:


# This script creates the 'gold_order_summary_daily' table
# It shows total quantity and sales revenue, aggregated by day, product name, product category, and vendor

orders_s_df = spark.read.table("silver_orders")
products_s_df = spark.read.table("silver_products")

orders_s_df.\
            join(
                products_s_df, 
                orders_s_df["product_id"] == products_s_df["product_id"], 
                "left"
                ).\
            groupBy(
                orders_s_df["order_date"],
                products_s_df["product_name"],
                products_s_df["product_category"],
                products_s_df["vendor"]
                ).\
            agg(
                sum("quantity").alias("quantity"),
                sum("order_total").alias("sales_revenue")
                ).\
            write.\
            mode("append").\
            saveAsTable("gold_order_summary_daily")
     

