-- Databricks notebook source
create database if not exists presentation

-- COMMAND ----------

show databases

-- COMMAND ----------

use database f1_processed

-- COMMAND ----------

create table if not exists presentation.calculated_race_results
using parquet
as
select races.race_year,
       constructor.name as teams_name,
       drivers.full_name as driver_name,
       results.position,
       results.points,
       11-results.position as calculated_points
from results
join drivers on (results.driver_id=drivers.driver_id)
join constructor on (results.constructor_id=constructor.constructor_Id)
join races on (results.race_id=races.race_Id) 

-- COMMAND ----------

Select * from presentation.calculated_race_results

-- COMMAND ----------


