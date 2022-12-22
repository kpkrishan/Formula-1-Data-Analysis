-- Databricks notebook source
show databases

-- COMMAND ----------

show tables in firstdatabase

-- COMMAND ----------

select * from firstdatabase.race_python

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####create Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC "or replace " will create or replace view for us an will not through error while runing the same statement again 

-- COMMAND ----------


create or replace temp view temp_view
as
Select * from firstdatabase.race_python 
where race_year=2020;

-- COMMAND ----------

select * from temp_view

-- COMMAND ----------



-- COMMAND ----------

create table firstdatabase.demotable
(empid int,
name String,
salary int
)

-- COMMAND ----------

insert into firstdatabase.demotable values(1,"krishan",2000),(2,"krishan",3000),(3,"krishan",4000),(4,"krishan",6000),(5,"krishan",5000),(6,"krishan",1000),(7,"krishan",8000),(8,"krishan",3000);

-- COMMAND ----------

select * from (select * from firstdatabase.demotable order by salary asc limit 2) order by salary desc limit 1

-- COMMAND ----------

create or replace temp view demo_view
as 
select * from firstdatabase.demotable order by salary asc limit 2

-- COMMAND ----------

select * from demo_view order by salary desc limit 1

-- COMMAND ----------


