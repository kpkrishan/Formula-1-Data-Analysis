# Databricks notebook source
# MAGIC %md
# MAGIC #results.json Ingestion

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

#defining the schema of the data
schema_result=StructType(fields=[StructField("constructorId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),False),
                                 StructField("fastestLap",IntegerType(),True),
                                 StructField("fastestLapSpeed",StringType(),True),
                                 StructField("fastestLapTime",StringType(),True),
                                 StructField("grid",IntegerType(),False),
                                 StructField("laps",IntegerType(),False),
                                 StructField("milliseconds",IntegerType(),True),
                                 StructField("number",IntegerType(),True),
                                 StructField("points",DoubleType(),False),
                                 StructField("position",IntegerType(),True),
                                 StructField("positionOrder",IntegerType(),False),
                                 StructField("positionText",StringType(),False),
                                 StructField("raceId",IntegerType(),False),
                                 StructField("rank",IntegerType(),True),
                                 StructField("resultId",IntegerType(),False),
                                 StructField("statusId",IntegerType(),False),
                                 StructField("time",StringType(),True)
                                
                                
                                ])

# COMMAND ----------

#read the data
result_data=spark.read.schema(schema_result).json(f"{raw_folder_path}/results.json")
display(result_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Drop unnecessary columns

# COMMAND ----------

dropped_data=result_data.drop(result_data.statusId)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Rename the columns if required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

#rename the columns

final_result=dropped_data.withColumnRenamed("constructorId","constructor_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumnRenamed("positionOrder","position_order").withColumnRenamed("positionText","position_text").withColumnRenamed("raceId","race_id").withColumnRenamed("resultId","result_id").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Add new Column

# COMMAND ----------

final_data_result=ingestion_date_func(final_result)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5: Write data into parquet file

# COMMAND ----------

#write the dataframe into parquet file
final_data_result.write.mode("overwrite").partitionBy("race_id").parquet(f"{process_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Create Table

# COMMAND ----------

final_data_result.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

dbutils.notebook.exit("Success!")
