# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest pit_stops.json

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_commands"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1: Read the Json file using dataframe Reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DoubleType

# COMMAND ----------

pit_schema=StructType(fields=[StructField("driverId",IntegerType(),True),
                                 StructField("duration",StringType(),True),
                                 StructField("lap",IntegerType(),True),
                                 StructField("milliseconds",IntegerType(),True),
                                 StructField("raceId",IntegerType(),False),
                                 StructField("stop",StringType(),True),
                                 StructField("time",StringType(),True)
                             ])


# COMMAND ----------

pit_data=spark.read.schema(pit_schema).option("multiLine",True).json("/mnt/racedata/rawdata/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Rename the columns 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

renamed_data=pit_data.withColumnRenamed("driverId","driver_id").withColumnRenamed("raceId","race_id").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Create New Column

# COMMAND ----------

pit_renamed_data=ingestion_date_func(renamed_data)

# COMMAND ----------

display(pit_renamed_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Write to parquet and save 

# COMMAND ----------

pit_renamed_data.write.mode("overwrite").parquet("/mnt/racedata/processeddata/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC #####create Table

# COMMAND ----------

pit_renamed_data.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success!")
