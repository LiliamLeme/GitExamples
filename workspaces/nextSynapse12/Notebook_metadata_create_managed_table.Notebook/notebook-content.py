# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "67f8983e-c811-4672-9b76-77704bf6075a",
# META       "default_lakehouse_name": "SQLDW",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


 df = spark.read.parquet("wasbs://filesystemdatalake@administrators.blob.core.windows.net/parquets/part-00001-bb8ab93a-d48a-4c78-836b-165bf6a92f4c-c000.snappy.parquet")
df.write.format("parquet").saveAsTable("SQLDW.parquets")





# CELL ********************

#--Create managed table using SparkSQL persisted on Table view, but not on Lake view. is that correct? managed tables different?

#--##Works if the container is using public access.
CREATE TABLE SQLDW.Person_deltaP
 USING delta
LOCATION 'wasbs://filesystemdatalake@administrators.blob.core.windows.net/Delta_Person/Person/'


#--https://msit-onelake.pbidedicated.windows.net/9fee2690-4084-4127-9ba5-0ca1b1180451/DimSalesReason_Delta/_delta_log?upn=false&action=getStatus&timeout=90
    CREATE TABLE SQLDW.Person_deltaP2
    USING delta
    LOCATION '/DimSalesReason_Delta/'
CREATE TABLE SQLDW.myexternalparquettable
    USING Parquet
    LOCATION 'wasbs://filesystemdatalake@administrators.blob.core.windows.net/parquets/part-00001-bb8ab93a-d48a-4c78-836b-165bf6a92f4c-c000.snappy.parquet'


# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC spark.sql("SHOW TABLES").show
# MAGIC 
# MAGIC spark.sql("DESCRIBE EXTENDED SQLDW.myexternalparquettable").show(truncate=false)


# METADATA ********************

# META {
# META   "language": "scala"
# META }

# CELL ********************

   CREATE TABLE SQLDW.myexternalparquettable
    USING Parquet
    LOCATION 'wasbs://filesystemdatalake@administrators.blob.core.windows.net/parquets/part-00001-bb8ab93a-d48a-4c78-836b-165bf6a92f4c-c000.snappy.parquet'
