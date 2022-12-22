# Databricks notebook source
# MAGIC %md
# MAGIC #####Join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df=spark.read.parquet(f"{process_folder_path}/races")

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_2019=race_df.filter(race_df["race_year"]==2019).withColumnRenamed("name","league_name")
display(race_2019)

# COMMAND ----------

circuit_df=spark.read.parquet(f"{process_folder_path}/circuits").withColumnRenamed("name","circuit_name")

# COMMAND ----------

display(circuit_df)

# COMMAND ----------

race_circuit_join=race_2019.join(circuit_df,race_2019.circuit_Id==circuit_df.circuit_Id,"inner").select(circuit_df.circuit_Id,circuit_df.location,circuit_df.circuit_name,race_2019.league_name)

# COMMAND ----------

display(race_circuit_join)

# COMMAND ----------


