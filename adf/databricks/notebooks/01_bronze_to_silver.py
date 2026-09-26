# Databricks notebook source
dbutils.widgets.text("file_name", "")
dbutils.widgets.text("sas_token", "")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("storage_account", "")

storage_account = dbutils.widgets.get("storage_account")
file_name = dbutils.widgets.get("file_name")
sas_token = dbutils.widgets.get("sas_token")
environment = dbutils.widgets.get("environment").lower()


# COMMAND ----------

allowed_environments = ["dev", "test", "prod"]

if environment not in allowed_environments:
    raise ValueError(
        f"Invalid environment: {environment}."
        f"Valid values: {allowed_environments}."
    )

# COMMAND ----------

# DBTITLE 1,Cell 2
container = "data"

url = (
    f"https://{storage_account}.blob.core.windows.net/"
    f"{container}/bronze/weather/{file_name}?{sas_token}"
)

# COMMAND ----------

catalog_name = f"weather_{environment}"
schema_name = "silver"
table_name = "weather"

full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

print(f"Full table name: {full_table_name}")

# COMMAND ----------

import requests

response = requests.get(url, timeout=30)
response.raise_for_status()


payload = response.json()

# COMMAND ----------

hours =  payload["forecast"]["forecastday"][0]["hour"]

rows = [
    {
        "timestamp": h["time"],
        "temperature_c": h["temp_c"],
        "humidity": h["humidity"],
        "wind_speed": h["wind_kph"],
        "condition": h["condition"][ "text"]
    }

    for h in hours
]

df = spark.createDataFrame(rows)

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = (
    df
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("ingestion_ts", F.current_timestamp())
)

# COMMAND ----------

silver_df.write.format("delta").mode("append").saveAsTable(full_table_name)