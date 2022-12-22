# Databricks notebook source
# MAGIC %md
# MAGIC temp view is not accessible from other notebooks. Also , Temp view is only available in spark session i.e. till the cluster is running.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Temp View
# MAGIC ####Access the dataframes using SQL
# MAGIC 
# MAGIC ######Create Temperory dataframes
# MAGIC ######Access the view using SQL
# MAGIC ######Access the View using Python

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df=spark.read.parquet(f"{process_folder_path}/races")

# COMMAND ----------

display(race_df)

# COMMAND ----------

result_df=spark.read.parquet(f"{process_folder_path}/results")

# COMMAND ----------

display(result_df)

# COMMAND ----------

race_result=result_df.join(race_df,result_df.race_id==race_df.race_Id)
display(race_result)

# COMMAND ----------

# MAGIC %md
# MAGIC #####1.create temp view on dataframe

# COMMAND ----------

race_result.createTempView("view_race_result")

# COMMAND ----------

# MAGIC %md
# MAGIC #####2.Access temp view using sql

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view_race_result

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,grid,laps,race_year,number,points, position,rank from view_race_result where race_year=2020

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Access the view using python

# COMMAND ----------

#while using sql in spark we can use external variables to assign the value but using Sql only we can not assign any value from external variable 
year=2012

# COMMAND ----------

df=spark.sql(f"Select * from view_race_result where race_year={year}")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Global Temp View
# MAGIC #####1. Create globaltemp dataframes

# COMMAND ----------

race_result.createOrReplaceGlobalTempView("gv_temp_view")

# COMMAND ----------

# MAGIC %md
# MAGIC #####2: Access thw view using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

#use global_temp as database name as global view is kind of table in the SQL


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_temp_view

# COMMAND ----------

# MAGIC %md
# MAGIC #####3. Access the view using python

# COMMAND ----------

data=spark.sql("select * from global_temp.gv_temp_view")

# COMMAND ----------

display(data)

# COMMAND ----------


