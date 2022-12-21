# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest drivers.json

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

driverdata=spark.read.json(f"{raw_folder_path}/drivers.json")
display(driverdata)

# COMMAND ----------

driverdata.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Perform Column operations

# COMMAND ----------

#let's create column out of struct column 
data_driver=driverdata.withColumn("forename",driverdata.name.forename).withColumn("surname",driverdata.name.surname)

# COMMAND ----------

display(data_driver)

# COMMAND ----------

#Concat the columns
from pyspark.sql.functions import concat,col,lit
concat_data=data_driver.withColumn("full_name",concat("forename",lit(" "),"surname"))

# COMMAND ----------

display(concat_data)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Drop Unnecessary Columns

# COMMAND ----------

#drop unnecessary columns
dropped_data=concat_data.drop("url","name","forename","surname")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Change the datatype where required

# COMMAND ----------

from pyspark.sql.types import DateType,IntegerType

# COMMAND ----------

#change the data type of column where required
data_type=dropped_data.withColumn("driverId",dropped_data.driverId.cast(IntegerType())).withColumn("dob",dropped_data.dob.cast(DateType()))


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5: Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_driver=data_type.withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(final_driver)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6: Add a new column

# COMMAND ----------

final_driver_df=ingestion_date_func(final_driver)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 7: Write data into parquet

# COMMAND ----------

final_driver_df.write.mode("overwrite").parquet(f"{process_folder_path}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC #####create table

# COMMAND ----------

final_driver_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success!")
