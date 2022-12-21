# Databricks notebook source
#dbutils.notebook.help()

# COMMAND ----------

result=dbutils.notebook.run("circuits_csv_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("race_csv_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("constructors_json_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("drivers_json_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("pitstop_json_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("result_json_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("laptime_folder_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------

result=dbutils.notebook.run("qualifying_folder_ingestion",0,{"p_data_source":"Ergast API"})

# COMMAND ----------

result

# COMMAND ----------


