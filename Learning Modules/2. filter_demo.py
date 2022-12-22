# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df=spark.read.parquet(f"{process_folder_path}/races")

# COMMAND ----------

display(race_df)

# COMMAND ----------

filter_race_df=race_df.filter((race_df["race_year"]==2019) | (race_df["race_year"]==2021))
display(filter_race_df)

# COMMAND ----------


