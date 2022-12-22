-- Databricks notebook source
select * 
from presentation.calculated_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dominating drivers of all time
-- MAGIC ---

-- COMMAND ----------

select driver_name, 
      count(1) as races_count, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points
from presentation.calculated_race_results 
group By driver_name
having races_count>50
order by average_points desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dominating driver between 2001 and 2010
-- MAGIC ---

-- COMMAND ----------

select driver_name, 
      count(1) as races_count, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points
from presentation.calculated_race_results 
where race_year between 2000 and 2010
group By driver_name
having races_count>50
order by average_points desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dominating driver between 2011 and 2020
-- MAGIC ---

-- COMMAND ----------

select driver_name, 
      count(1) as races_count, 
      sum(calculated_points) as total_points, 
      avg(calculated_points) as average_points
from presentation.calculated_race_results 
where race_year between 2011 and 2020
group By driver_name
having races_count>50
order by average_points desc

-- COMMAND ----------


