# Databricks notebook source
# DBTITLE 1,Step 1: External Locations (Raw S3 Data)
# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS batch_external_location
# MAGIC   URL 's3://data-batch-dbr/'
# MAGIC   WITH (STORAGE CREDENTIAL credential_dbr);
# MAGIC
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS streaming_external_location
# MAGIC   URL 's3://data-streaming-dbr/'
# MAGIC   WITH (STORAGE CREDENTIAL credential_dbr);
# MAGIC
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# DBTITLE 1,Step 2: Create Catalog and Schemas
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS  retail_rocket;
# MAGIC
# MAGIC USE CATALOG retail_rocket;
# MAGIC
# MAGIC -- Ingestion layer: doc raw data tu S3
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS serving;

# COMMAND ----------

# DBTITLE 1,Step 3: Create Checkpoint Volumes
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS retail_rocket.bronze.checkpoints;
# MAGIC CREATE VOLUME IF NOT EXISTS retail_rocket.silver.checkpoints;
# MAGIC
# MAGIC SHOW VOLUMES IN retail_rocket.bronze;
# MAGIC -- SHOW VOLUMES IN retail_rocket.silver;