# Databricks notebook source
# MAGIC %md
# MAGIC #####Aggregate Functions

# COMMAND ----------

presentation=spark.read.parquet("/mnt/presentation/race_result_2020")
display(presentation)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum

# COMMAND ----------

# MAGIC %md
# MAGIC ######Count()

# COMMAND ----------

presentation.select("*").count()

# COMMAND ----------

presentation.select(sum(presentation.points)).show()

# COMMAND ----------

presentation.filter(presentation.full_name=="Lewis Hamilton").select("points").show()

# COMMAND ----------

presentation.select("nationality").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #####groupBy

# COMMAND ----------

presentation.groupBy("nationality").count().show()

# COMMAND ----------

#agg() takes expressions of agg functions
presentation.groupBy("nationality").agg(sum("points")).show()

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_df=spark.read.parquet(f"{process_folder_path}/races")

# COMMAND ----------

display(race_df)

# COMMAND ----------

result_df=spark.read.parquet(f"{process_folder_path}/results")
display(result_df)

# COMMAND ----------

race_result=race_df.join(result_df,race_df.race_Id==result_df.race_id)
display(race_result)

# COMMAND ----------



# COMMAND ----------

driver_df=spark.read.parquet(f"{process_folder_path}/drivers")
display(driver_df)

# COMMAND ----------

driver_race=driver_df.join(race_result,driver_df.driver_id==race_result.driver_id)
display(driver_race)

# COMMAND ----------

final_df=driver_race.select("nationality","full_name","name","round","race_year","points","position","rank","time")

# COMMAND ----------

display(final_df)

# COMMAND ----------

data_19and20=final_df.filter((final_df["race_year"]==2019) | (final_df["race_year"]==2020))

# COMMAND ----------

display(data_19and20)

# COMMAND ----------

group_df=data_19and20.groupBy("race_year","full_name").agg(count("full_name")\
.alias("driver name"),sum("points")\
.alias("total_points"))\
.orderBy("total_points")

# COMMAND ----------

display(group_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Window Functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc,rank

# COMMAND ----------

#step1: crate window

mywindow=Window.partitionBy("race_year").orderBy(desc("total_points"))

#step2: create new column of having window function applied
group_df.withColumn("rank",rank().over(mywindow)).show(100)

# COMMAND ----------


