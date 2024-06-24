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

##Reading the data imported from the pipeline into the workspace

##%%pyspark - UsageError: Line magic function `%%pyspark` not found.
df = spark.read.format("csv").option("header","true").load("Files/Files/FactInternetSales/part-00000-871b5a9d-1169-42bb-a536-66a131696b53-c000.csv")
# df now is a Spark DataFrame containing CSV data from Files/Files/FactInternetSales/part-00000-871b5a9d-1169-42bb-a536-66a131696b53-c000.csv.
display(df)

# CELL ********************

import pandas as pd


# Load data into pandas DataFrame from Files/Files/FactInternetSalesReason/part-00001-32dc77cc-4e5c-4b16-8bfb-f37dad68f91f-c000.csv
df = pd.read_csv("/lakehouse/default/Files/Files/FactInternetSalesReason/part-00001-32dc77cc-4e5c-4b16-8bfb-f37dad68f91f-c000.csv")

display(df)


# CELL ********************

df = spark.read.parquet("Files/Files/FactInternetSales_Delta/OrderDateKey=20010703/part-00000-3fa7ad01-1990-4039-9f04-d3972427e1ed.c000.snappy.parquet")
# df now is a Spark DataFrame containing parquet data from Files/Files/FactInternetSales_Delta/OrderDateKey=20010703/part-00000-3fa7ad01-1990-4039-9f04-d3972427e1ed.c000.snappy.parquet.
display(df)

# CELL ********************

##read and write and create folder

df = spark.read.parquet("Files/Files/FactInternetSales_Delta/OrderDateKey=20010703/part-00000-3fa7ad01-1990-4039-9f04-d3972427e1ed.c000.snappy.parquet")
df.write.option("maxRecordsPerFile", 90).mode("overwrite").parquet("Files/Files/FactInternetSales_Delta/OrderDateKey=20010703/write_tst/part-00000-3fa7ad01-1990-4039-9f04-d3972427e1ed.c000.snappy.parquet")


# CELL ********************

import json 
import pandas as pd 
from pandas.io.json import json_normalize 
  
with open('https://administrators.blob.core.windows.net/filesystemdatalake/JSONHack/json/2023-01-29/19/0_2c8abee0ee80439da2939f3f26a02c1e_1.json') as f:
    d = json.load(f)
  


