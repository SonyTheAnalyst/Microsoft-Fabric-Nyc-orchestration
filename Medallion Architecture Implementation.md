# Medallion Architecture Implementation

A structured implementation of the Medallion (Bronze → Silver → Gold) lakehouse architecture using PySpark on Microsoft Fabric / Azure Databricks. Each layer refines and enriches the data, progressively improving quality and usability.

## Links and Resources

- [Describe Medallion Architecture – Microsoft Learn](https://learn.microsoft.com/en-us/training/modules/describe-medallion-architecture/2-describe-medallion-architecture)
- [OneLake Medallion Lakehouse Architecture](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture)

---

## Table of Contents

1. [Overview](#overview)
2. [Utilities / Imports](#utilities--imports)
3. [Bronze Layer](#bronze-layer)
   - [Orders](#bronze--orders)
   - [Products](#bronze--products)
   - [Reviews](#bronze--reviews)
   - [Users](#bronze--users)
4. [Silver Layer](#silver-layer)
   - [Orders](#silver--orders)
   - [Products](#silver--products)
   - [Reviews](#silver--reviews)
   - [Users](#silver--users)
5. [Gold Layer](#gold-layer)
   - [Daily Order Summary](#gold--daily-order-summary)
   - [Top Rated Products](#gold--top-rated-products)
   - [User Purchase Summary](#gold--user-purchase-summary)
6. [Best Practices](#best-practices)

---

## Overview

| Layer  | Purpose | Tables |
|--------|---------|--------|
| **Bronze** | Raw ingestion — data is loaded as-is with a `date_processed` audit column | `bronze_orders`, `bronze_products`, `bronze_reviews`, `bronze_users` |
| **Silver** | Cleaned and conformed — columns are renamed, types cast, and derived fields calculated | `silver_orders`, `silver_products`, `silver_reviews`, `silver_users` |
| **Gold** | Aggregated and analytics-ready — joined and summarised for BI / reporting | `gold_order_summary_daily`, `gold_top_rated_products`, `gold_user_purchase_summary` |

---

## Utilities / Imports

```python
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, date_format, round, sum
)
```

---

## Bronze Layer

The Bronze layer is the **raw ingestion zone**. Data is read directly from source files and written to Delta tables with minimal transformation. An audit column `date_processed` is added to every record so lineage can be traced.

> **Idempotency note:** All writes use `mode("append")`. To make ingestion idempotent, consider adding a deduplication step or switching to `mode("overwrite")` with partition pruning.

### Bronze – Orders

```python
# Path to the orders Parquet files (may be a folder of part files)
path_orders = "Files/retail_dataset_de/orders"

# Read raw Parquet, stamp with ingestion time, and append to the bronze table
spark \
    .read \
    .format("parquet") \
    .load(path_orders) \
    .withColumn("date_processed", current_timestamp()) \
    .write \
    .mode("append") \
    .saveAsTable("bronze_orders")
```

### Bronze – Products

```python
# Path to the products Parquet file
path_products = "Files/retail_dataset_de/products.parquet"

# Read raw Parquet, stamp with ingestion time, and append to the bronze table
spark \
    .read \
    .format("parquet") \
    .load(path_products) \
    .withColumn("date_processed", current_timestamp()) \
    .write \
    .mode("append") \
    .saveAsTable("bronze_products")
```

### Bronze – Reviews

```python
# Path to the reviews JSON file
path_reviews = "Files/retail_dataset_de/reviews.json"

# Explicit schema prevents schema inference overhead and guards against source drift
schema_json = (
    "id INT, created_at TIMESTAMP, reviewer STRING, "
    "product_id INT, rating DECIMAL(5,2), body STRING"
)

# Read JSON with enforced schema, stamp with ingestion time, and append to the bronze table
spark \
    .read \
    .schema(schema_json) \
    .format("json") \
    .load(path_reviews) \
    .withColumn("date_processed", current_timestamp()) \
    .write \
    .mode("append") \
    .saveAsTable("bronze_reviews")
```

### Bronze – Users

```python
# Path to the users CSV file
path_users = "Files/retail_dataset_de/users.csv"

# Explicit schema ensures correct types; avoids costly inferSchema scan on large files
schema_users = (
    "id INT, created_at TIMESTAMP, name STRING, email STRING, "
    "city STRING, state STRING, zip STRING, birth_date DATE, source STRING"
)

# Read CSV with header and enforced schema, stamp with ingestion time,
# and append to the bronze table
spark \
    .read \
    .option("header", "true") \
    .schema(schema_users) \
    .format("csv") \
    .load(path_users) \
    .withColumn("date_processed", current_timestamp()) \
    .write \
    .mode("append") \
    .saveAsTable("bronze_users")
```

---

## Silver Layer

The Silver layer is the **cleaned and conformed zone**. Columns are renamed for consistency, data types are cast, derived fields are calculated, and only relevant columns are selected. No raw source columns are dropped entirely — they are reshaped for downstream use.

> **Data quality note:** Consider adding `dropDuplicates()` or `filter` steps here to remove duplicate records introduced during incremental ingestion.

### Silver – Orders

```python
# Read from bronze, derive order_total and a clean order_date, then select analytics columns
spark \
    .read \
    .table("bronze_orders") \
    .withColumn(
        "order_total",
        round(col("quantity") * col("unit_price"), 2)  # calculated line-item total
    ) \
    .withColumn(
        "order_date",
        date_format(col("created_at"), "yyyy-MM-dd").cast("date")  # normalise to DATE type
    ) \
    .select(
        col("id"),
        col("order_date"),
        col("user_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        col("order_total"),
        col("date_processed")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("silver_orders")
```

### Silver – Products

```python
# Read from bronze, rename columns for clarity, and write to silver
spark \
    .read \
    .table("bronze_products") \
    .select(
        col("id").alias("product_id"),        # standardise primary key name
        col("created_at"),
        col("title").alias("product_name"),   # descriptive rename
        col("category").alias("product_category"),
        col("ean"),
        col("vendor"),
        col("date_processed")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("silver_products")
```

### Silver – Reviews

```python
# Read from bronze and promote to silver with a standardised schema
spark \
    .read \
    .table("bronze_reviews") \
    .select(
        col("id").alias("review_id"),
        col("created_at").alias("review_date"),
        col("reviewer"),
        col("product_id"),
        col("rating"),
        col("body").alias("review_body"),
        col("date_processed")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("silver_reviews")
```

### Silver – Users

```python
# Read from bronze and promote to silver with a standardised schema
spark \
    .read \
    .table("bronze_users") \
    .select(
        col("id").alias("user_id"),
        col("created_at"),
        col("name"),
        col("email"),
        col("city"),
        col("state"),
        col("zip"),
        col("birth_date"),
        col("source"),
        col("date_processed")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("silver_users")
```

---

## Gold Layer

The Gold layer is the **analytics-ready zone**. Data from the Silver layer is joined, aggregated, and shaped into wide tables that power dashboards and reports. These tables are optimised for read performance.

> **Performance note:** For large datasets, consider partitioning Gold tables by `order_date` or using `OPTIMIZE` / `ZORDER` on frequently filtered columns.

### Gold – Daily Order Summary

Aggregates total quantity sold and sales revenue by day, product, category, and vendor.

```python
# Load silver tables into DataFrames for the join
orders_s_df = spark.read.table("silver_orders")
products_s_df = spark.read.table("silver_products")

# Join orders → products, then aggregate to daily grain
orders_s_df \
    .join(
        products_s_df,
        orders_s_df["product_id"] == products_s_df["product_id"],
        "left"  # keep all orders even if product lookup is missing
    ) \
    .groupBy(
        orders_s_df["order_date"],
        products_s_df["product_name"],
        products_s_df["product_category"],
        products_s_df["vendor"]
    ) \
    .agg(
        sum("quantity").alias("quantity"),
        sum("order_total").alias("sales_revenue")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("gold_order_summary_daily")
```

### Gold – Top Rated Products

Ranks products by average customer rating and review volume.

```python
# Load silver tables
reviews_s_df = spark.read.table("silver_reviews")
products_s_df = spark.read.table("silver_products")

# Aggregate review statistics per product, then join product metadata
reviews_s_df \
    .groupBy("product_id") \
    .agg(
        avg("rating").alias("avg_rating"),    # mean star rating
        count("review_id").alias("review_count")
    ) \
    .join(
        products_s_df,
        "product_id",
        "left"
    ) \
    .select(
        col("product_id"),
        col("product_name"),
        col("product_category"),
        col("vendor"),
        round(col("avg_rating"), 2).alias("avg_rating"),
        col("review_count")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("gold_top_rated_products")
```

### Gold – User Purchase Summary

Summarises lifetime purchase behaviour per user for customer analytics.

```python
# Load silver tables
orders_s_df = spark.read.table("silver_orders")
users_s_df = spark.read.table("silver_users")

# Aggregate order metrics per user, then enrich with user profile
orders_s_df \
    .groupBy("user_id") \
    .agg(
        count("id").alias("total_orders"),
        sum("order_total").alias("lifetime_spend"),
        round(avg("order_total"), 2).alias("avg_order_value")
    ) \
    .join(
        users_s_df,
        "user_id",
        "left"
    ) \
    .select(
        col("user_id"),
        col("name"),
        col("city"),
        col("state"),
        col("total_orders"),
        col("lifetime_spend"),
        col("avg_order_value")
    ) \
    .write \
    .mode("append") \
    .saveAsTable("gold_user_purchase_summary")
```

---

## Best Practices

### Idempotency
- Use `mode("overwrite")` with partition filters, or Delta Lake `MERGE` (`merge`/`upsert`) to avoid duplicating records on re-runs.
- Add a watermark or high-watermark column (e.g. `max(date_processed)`) to implement incremental loads.

### Error Handling
- Wrap each layer's write operation in a `try/except` block and log failures to a dedicated audit table.
- Use `badRecordsPath` option when reading JSON/CSV to capture malformed records instead of failing the entire job.

### Performance
- Define explicit schemas (as shown in the Bronze layer) to avoid expensive `inferSchema` scans.
- Co-locate frequently joined keys (e.g. `product_id`, `user_id`) using Delta `ZORDER BY` for faster lookups.
- Cache intermediate DataFrames (`.cache()`) if they are reused across multiple Gold aggregations in the same session.

### Data Quality
- Add `dropDuplicates(["id", "date_processed"])` in the Silver layer to prevent duplicate rows from incremental Bronze loads.
- Validate nullable constraints and value ranges (e.g. `rating BETWEEN 0 AND 5`) before writing to Silver.

