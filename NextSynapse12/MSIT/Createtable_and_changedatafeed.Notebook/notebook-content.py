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

--Create table using Spark SQL persisted on lake view


--DROP TABLE SQLDW.FactInternetSales;

CREATE TABLE SQLDW.FactInternetSales
(ProductKey int,	OrderDateKey int,DueDateKey int,ShipDateKey int ,CustomerKey int ,PromotionKey int ,CurrencyKey int ,SalesTerritoryKey int ,SalesOrderNumber string ,	SalesOrderLineNumber smallint ,	RevisionNumber tinyint ,	OrderQuantity smallint ,	UnitPrice float ,	ExtendedAmount float ,	UnitPriceDiscountPct float ,	DiscountAmount float ,	ProductStandardCost float ,	TotalProductCost float ,	SalesAmount float ,	TaxAmt float ,	Freight float ,	CarrierTrackingNumber string ,CustomerPONumber string) ;







# CELL ********************

import json

# Opening JSON file
f = open('data.json',)

# returns JSON object as
# a dictionary
data = json.load(f)

# Iterating through the json
# list
for i in data['emp_details']:
print(i)

# CELL ********************

##https://www.serverlesssql.com/delta-change-data-feed-in-fabric-lakehouses/

spark.conf.set("spark.microsoft.delta.properties.defaults.enableChangeDataFeed", "true")

# CELL ********************

##https://www.serverlesssql.com/delta-change-data-feed-in-fabric-lakehouses/

#import data types
from pyspark.sql.types import *
from datetime import datetime

#create schema
table_schema = StructType([
                    StructField('OrderID', IntegerType(), True),
                    StructField('ProductName', StringType(), True),
                    StructField('ItemPrice', IntegerType(), True),
                    StructField('OrderTotal', IntegerType(), True),
                    StructField('OrderDate', DateType(), True)])

#load rows
staged_rows = [(1,'Soft Toy',10, 35,datetime(2023, 11, 20)),
            (2,'Mobile Phone',450, 10,datetime(2023, 11, 20)),
            (3,"Notepad",5,125,datetime(2023, 11, 20))]

#create dataframe and append current datetime
staged_df = spark.createDataFrame(staged_rows,table_schema) \
            .write.mode("overwrite").format("delta").save("Tables/rawproductsales")

# CELL ********************

#read data
df = spark.read.format("delta").table("rawproductsales")
display(df)

# METADATA ********************

# META {}

# CELL ********************

#add new order
new_order = [(4,'TV',2, 750,datetime(2023, 11, 21))]

spark.createDataFrame(data=new_order, schema = table_schema).write.format("delta").mode("append").saveAsTable("rawproductsales")

# CELL ********************

#add new order
new_order = [(4,'TV',2, 750,datetime(2023, 11, 21))]

spark.createDataFrame(data=new_order, schema = table_schema).write.format("delta").mode("append").saveAsTable("rawproductsales")

# CELL ********************

#add new order
new_order = [(4,'TV',2, 750,datetime(2023, 11, 21))]

spark.createDataFrame(data=new_order, schema = table_schema).write.format("delta").mode("append").saveAsTable("rawproductsales")

# CELL ********************


#read change data feed
changedatefeed_df = spark.read.format("delta") \
    .option("readChangeData", True) \
    .option("startingVersion", 0) \
    .table('rawproductsales')

display(changedatefeed_df.sort("_commit_version"))

# CELL ********************

# MAGIC %%sql
# MAGIC --get the updates only
# MAGIC SELECT *
# MAGIC FROM table_changes('rawproductsales', 1)
# MAGIC WHERE _change_type ='update_postimage'

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }
