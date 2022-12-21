# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest laptimes folder

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_commands"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1: Read the file using dataframe Reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType,StringType

# COMMAND ----------

laptime_schema=StructType(fields=[StructField("raceId",IntegerType(), False),
                                  StructField("driverId",IntegerType(), False),
                                  StructField("lap",IntegerType(), False),
                                  StructField("position",IntegerType(), False),
                                  StructField("time",StringType(), False),
                                  StructField("milliseconds",IntegerType(), False),
    
])

# COMMAND ----------

lap_data=spark.read.schema(laptime_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Rename the columns if required

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

final_data=lap_data.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("data_source",lit(v_data_source))


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Add new column 

# COMMAND ----------

final_write_data=ingestion_date_func(final_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Write data file into parquet folder

# COMMAND ----------

#write to parquet

final_write_data.write.mode("overwrite").parquet(f"{process_folder_path}/laptimes")

# COMMAND ----------

# MAGIC %md
# MAGIC #####create Table

# COMMAND ----------

final_write_data.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")

# COMMAND ----------

dbutils.notebook.exit("Success!")
