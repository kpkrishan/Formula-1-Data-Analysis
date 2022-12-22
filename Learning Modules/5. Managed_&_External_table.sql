-- Databricks notebook source
-- MAGIC %md
-- MAGIC #####SQL Commands

-- COMMAND ----------

create database if not exists firstdatabase;

-- COMMAND ----------

show databases;

-- COMMAND ----------

use database default;

-- COMMAND ----------

show tables

-- COMMAND ----------

select current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Managed Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1.Using Python

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_df= spark.read.parquet(f"{process_folder_path}/races")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######saving data as table 

-- COMMAND ----------

use database firstdatabase

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_df.write.format("parquet").saveAsTable("race_python")

-- COMMAND ----------

show databases

-- COMMAND ----------

use firstdatabase

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------

use database default;

-- COMMAND ----------

show tables;

-- COMMAND ----------

select * from firstdatabase.race_python;

-- COMMAND ----------

desc extended firstdatabase.race_python;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Using SQL

-- COMMAND ----------

show databases

-- COMMAND ----------

use database firstdatabase


-- COMMAND ----------

show tables

-- COMMAND ----------

create table race_sql
as
select * from firstdatabase.race_python
where race_year=2020

-- COMMAND ----------

desc extended race_sql

-- COMMAND ----------

show tables in firstdatabase;

-- COMMAND ----------

drop table race_sql;

-- COMMAND ----------

show tables in firstdatabase;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create External Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_df.write.format("parquet").option("path","/mnt/presentation/race_python_ext").saveAsTable("race_python_ext_table")

-- COMMAND ----------

desc extended race_python_ext_table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Using SQL
-- MAGIC We need to specify the location of the External tables

-- COMMAND ----------

create table firstdatabase.race_sql_ext_table
(std_id Int,
name String,
class String,
subject String,
marks int
)
 using parquet
 location "/mnt/presentation/race_sql_ext_table"

-- COMMAND ----------

insert into firstdatabase.race_sql_ext_table values(1,"krishan","second","Hindi",90),(2,"krishan","third","Hindi",90),(3,"krishan","fourth","English",90),(4,"Akash","second","Maths",40),(5,"krishan","second","English",90);

-- COMMAND ----------


