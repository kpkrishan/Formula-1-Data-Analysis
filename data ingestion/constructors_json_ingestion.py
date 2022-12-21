# Databricks notebook source
# MAGIC %md
# MAGIC ###json Data Ingestion-->constructors.json
# MAGIC 
# MAGIC json--->read--->Transform--->Write

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------



# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_commands"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1: Read the Json file using dataframe Reader

# COMMAND ----------

#define Schema
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

schema=StructType(fields=[StructField("constructorId",IntegerType(),False),
                          StructField("constructorRef",StringType(),True),
                          StructField("name",StringType(),True),
                          StructField("nationality",StringType(),True),
                          StructField("url",StringType(),True),
                         
                         ])

# COMMAND ----------

read_json=spark.read.schema(schema).json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(read_json)

# COMMAND ----------

read_json.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Drop unnecessary columns

# COMMAND ----------

#drop url column
construct_data=read_json.drop(read_json.url)

# COMMAND ----------

display(construct_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Rename the columns if required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df=construct_data.withColumnRenamed("constructorId","constructor_Id").withColumnRenamed("constructorRef","constructor_Ref").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Add new Column

# COMMAND ----------

construct_final=ingestion_date_func(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5: Write data into parquet file

# COMMAND ----------

construct_final.write.mode("overwrite").parquet(f"{process_folder_path}/constructor")

# COMMAND ----------

# MAGIC %md
# MAGIC #####create table 

# COMMAND ----------

construct_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

dbutils.notebook.exit("Success!")
