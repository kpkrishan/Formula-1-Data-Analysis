# Databricks notebook source
# MAGIC %md
# MAGIC #Csv Data ingestion
# MAGIC 
# MAGIC CSV---> Read--->Transform--->Write

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

#this command is to run configuration notebook inside the circuits_csv_ingestion notebook.
#".." is used to backtrack the folder and go to next folder where configuration notebook is saved, i.e includes



# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_commands"

# COMMAND ----------

# MAGIC %md
# MAGIC ###races.csv Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1: Read the csv file using Dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

#manually define the column's datatype
col_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                              StructField("year",IntegerType(),True),
                              StructField("round",IntegerType(),True),
                              StructField("cercuitId",IntegerType(),True),
                              StructField("name",StringType(),True),
                              StructField("date",StringType(),True),
                              StructField("time",StringType(),True)
                             ])

# COMMAND ----------

#read race.csv 
race_df=spark.read.schema(col_schema).csv(f"{raw_folder_path}/races.csv",header=True)
display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Field Level transformations

# COMMAND ----------

from pyspark.sql.functions import lit,to_timestamp,concat,col,current_timestamp

# COMMAND ----------

#concatinate the date and time col and convert it to timestamp

race_df=race_df.withColumn("date_time",to_timestamp(concat(race_df["date"],lit(' '),race_df["time"]),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

#add new column: ingestion_date
race_add_column=ingestion_date_func(race_df)

# COMMAND ----------

#rename the columns where required
race_transformed=race_add_column.withColumnRenamed("raceId","race_Id").withColumnRenamed("year","race_year").withColumnRenamed("cercuitId","circuit_Id").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(race_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Field Selection

# COMMAND ----------

final_race=race_transformed.select("race_Id","race_year","round","circuit_Id","name","date_time","ingestion_date")
display(final_race)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write file into parquet format and save to procesddeddata folder

# COMMAND ----------

#write into parquet and partitionBy the race year
final_race.write.mode("overwrite").partitionBy("race_year").parquet(f"{process_folder_path}/races")

# COMMAND ----------

#display(spark.read.parquet(f"{process_folder_path}/races"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####create table

# COMMAND ----------

final_race.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success!")
