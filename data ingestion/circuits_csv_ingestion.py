# Databricks notebook source
# MAGIC %md
# MAGIC #Csv Data ingestion
# MAGIC 
# MAGIC CSV---> Read--->Transform--->Write

# COMMAND ----------

# MAGIC %md
# MAGIC ##circuits.csv Ingestion

# COMMAND ----------

#dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC this command is to run configuration notebook inside the circuits_csv_ingestion notebook.
# MAGIC ".." is used to backtrack the folder and go to next folder where configuration notebook is saved, i.e includes

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_commands"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1: Read the csv file using dataframe reader

# COMMAND ----------

#Checking the mounting location for csv file
display(dbutils.fs.mounts())

# COMMAND ----------

# #read the csv file using dataframe reader 
# circuit=spark.read.option("inferSchema",True).csv("/mnt/racedata/rawdata/circuits.csv",header=True)
# circuit.show()

# COMMAND ----------

#Since inferSchema is not efficient to deal with the data as it goes to each row to identify the datatype of the column
#hence we will use structType() to specify our own datatype for each column

from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DoubleType

# COMMAND ----------

#using StrcutType we are explictly defining the schema of the table
schema_circuit=StructType(fields=[StructField("circuitId",IntegerType(),False),
                                 StructField("circuitRef",StringType(),True),
                                 StructField("name",StringType(),True),
                                 StructField("location",StringType(),True),
                                 StructField("country",StringType(),True),
                                 StructField("lat",DoubleType(),True),
                                 StructField("lng",DoubleType(),True),
                                 StructField("alt",IntegerType(),True),
                                 StructField("url",StringType(),True),
    
])

# COMMAND ----------

#adding folder path from the child notebook
circuit=spark.read.schema(schema_circuit).csv(f"{raw_folder_path}/circuits.csv",header=True)
circuit.show()

# COMMAND ----------

#Print the schema of the dataset
#if schema is not correct then use "inferSchema=True" while reading the file itself
circuit.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Select only required columns

# COMMAND ----------

#To display all the columns 
circuit_all_df=circuit.select("*")

# COMMAND ----------

#droping "url" column 
#First: providing column  name 
#we can not apply column based functions here, like to change the name of the column using alias
circuit_selected_df=circuit.select("circuitId","circuitRef","name","location","country","lat","lng","alt")


# COMMAND ----------

circuit_selected_df.show()

# COMMAND ----------

#Second: we can fetch the column names using dot natation
#we can use column functions 
circuit_selected_df=circuit.select(circuit.circuitId,circuit.circuitRef,circuit.name,circuit.location,circuit.country,circuit.lat,circuit.lng,circuit.alt)

# COMMAND ----------

#Third: Another way of providing column
#we can use column functions 
circuit_selected_df=circuit.select(circuit["circuitId"],circuit["circuitRef"],circuit["name"],circuit["location"],circuit["lat"],
                                   circuit["lng"],circuit["alt"])

# COMMAND ----------

display(circuit_selected_df)

# COMMAND ----------

#fourth: using col fucntion
#we can use column functions 
from pyspark.sql.functions import col
circuit_selected_df=circuit.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("lat").alias("latitude"),
                                   col("lng").alias("longitude"),col("alt").alias("altitude"))
circuit_selected_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 3: Rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#withColumnRenamed("Existing_colname","NewCol_name")
circuit_selected_df=circuit_selected_df.withColumnRenamed("circuitId","circuit_Id").withColumnRenamed("circuitRef","circuit_Ref").withColumn("data_source",lit(v_data_source))

# COMMAND ----------

circuit_selected_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 4: Add a new column

# COMMAND ----------

#adding a new column
#withColumn("column_name",value)
#for literal values...import lit from functions and wrap it with lit()

final_circuit=ingestion_date_func(circuit_selected_df)

# COMMAND ----------

display(final_circuit)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5: Write the file into parquet and save to processeddata folder

# COMMAND ----------

#write the final dataframe into parquet file in the processeddata folder in the container
final_circuit.write.mode("overwrite").parquet(f"{process_folder_path}/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #####create Table

# COMMAND ----------

final_circuit.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success!")

# COMMAND ----------


