# Databricks notebook source
# MAGIC %md
# MAGIC ###Intesting qualifying folder

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

qualifying_schema=StructType(fields=[StructField("qualifyId",IntegerType(), False),
                                  StructField("raceId",IntegerType(), False),
                                  StructField("driverId",IntegerType(), False),
                                  StructField("constructorId",IntegerType(), False),
                                  StructField("number",IntegerType(), True),
                                  StructField("position",IntegerType(), True),
                                  StructField("q1",StringType(), True),
                                  StructField("q2",StringType(), True),
                                  StructField("q3",StringType(), True)
])

# COMMAND ----------

qual_data=spark.read.option("multiLine",True).schema(qualifying_schema).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qual_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Rename the columns if required

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

final_qualify=qual_data.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Add new column 

# COMMAND ----------


final_qualify_data=ingestion_date_func(final_qualify)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Write data file into parquet folder

# COMMAND ----------

final_qualify_data.write.mode("overwrite").parquet(f"{process_folder_path}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #####create Table

# COMMAND ----------

final_qualify_data.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success!")
