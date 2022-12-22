-- Databricks notebook source
-- MAGIC %md
-- MAGIC Dominating teams
-- MAGIC ---

-- COMMAND ----------

select *     
from presentation.calculated_race_results 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Dominating of all time

-- COMMAND ----------

select teams_name,
       count(1) as total_count,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
        
from presentation.calculated_race_results
group by teams_name
having total_count >=100
order by avg_points desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Dominating team of decade 2001 and 2010

-- COMMAND ----------

select teams_name,
       count(1) as total_count,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
        
from presentation.calculated_race_results
where race_year between 2001 and 2010
group by teams_name
having total_count >=100
order by avg_points desc

-- COMMAND ----------


