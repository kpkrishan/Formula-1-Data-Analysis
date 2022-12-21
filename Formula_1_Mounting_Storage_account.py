# Databricks notebook source
#Mounting Storage Account


#storage Account name
storageAccountName="substorageaccountsa"

#Container name
containerName="raceproject"


# COMMAND ----------

storageAccAccessKey=dbutils.secrets.get(scope="subscope",key="subsstoragekey")

configs={
    "fs.azure.account.key."+storageAccountName+".blob.core.windows.net":storageAccAccessKey
}

# COMMAND ----------

dbutils.fs.mount( 
    source = "wasbs://"+containerName+"@"+storageAccountName+".blob.core.windows.net/", 
    mount_point = "/mnt/racedata", 
    extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#unmounting the previous mounts
# dbutils.fs.unmount("/mnt/racedata")

# COMMAND ----------

dbutils.fs.ls("/mnt/racedata/rawdata")

# COMMAND ----------


