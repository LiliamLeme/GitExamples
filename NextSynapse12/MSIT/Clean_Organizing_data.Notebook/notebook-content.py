# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "93b443f5-777a-4196-9e74-d4aea2ca4700",
# META       "default_lakehouse_name": "SQLDB_Synapse",
# META       "default_lakehouse_workspace_id": "e39e052f-46ae-4759-91bd-c810a117a436",
# META       "known_lakehouses": [
# META         {
# META           "id": "93b443f5-777a-4196-9e74-d4aea2ca4700"
# META         },
# META         {
# META           "id": "ec7e333c-a646-475e-8316-fe2a234bf38d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Reading the parquet file from Source.  Joining, summarizing, handling nulls and saving s table

# CELL ********************

df = spark.sql("SELECT * FROM SQLDB_Synapse.product_purchase_consolidate")
display(df)
##https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/low-shuffle-merge-for-apache-spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.cache()
df.count()
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
#import pandas 

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable VOrder write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write
#https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order?tabs=sparksql

# Read the PurchaseOrderDetail and Product tables
purchase_order_detail = spark.read.load('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Purchasing.PurchaseOrderDetail/*.parquet', format='parquet')
product = spark.read.load('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.Product/*.parquet', format='parquet')


# Perform the join and aggregation
join_result = purchase_order_detail.join(product, "ProductID") \
    .groupBy(purchase_order_detail.ModifiedDate, purchase_order_detail.ProductID, product.ProductNumber, product.MakeFlag,\
     product.FinishedGoodsFlag, product.Color, product.SafetyStockLevel) \
    .agg(
        sum("UnitPrice").alias("UnitPrice"),
        sum("OrderQty").alias("OrderQty"),
        sum("StockedQty").alias("StockedQty")
    )


# Fill NA values in the "Color" column
join_result = join_result.fillna("No Values/Unknow", subset=["Color"])

join_result.show()

join_result.write.format("delta").mode("overwrite").saveAsTable("Product_Purchase_Consolidate2")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##custom library.
from math_custom import square, cube
                
print (square.square(2))
print (cube.cube(2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable VOrder write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write

df_Product = spark.read.parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.Product/*.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.Product/part-00000-854e8215-a88d-45f3-90a7-85721c25a05b-c000.snappy.parquet".
display(df_Product)
df_Product.write.format("delta").mode("overwrite").saveAsTable("Product")


df_ProductCostHistory = spark.read.parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.ProductCostHistory/*.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.ProductCostHistory/part-00000-7604a114-b64e-4959-8328-6d85bfd11a24-c000.snappy.parquet".
display(df_ProductCostHistory)
df_ProductCostHistory.write.format("delta").mode("overwrite").saveAsTable("ProductCostHistory")

df_ProductDescription = spark.read.parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.ProductDescription/*.parquet")
# df now is a Spark DataFrame containing parquet data from "abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.ProductDescription/part-00000-0b4e251d-b101-4d10-8d58-cae0e8de5c09-c000.snappy.parquet".
display(df_ProductDescription)
df_ProductDescription.write.format("delta").mode("overwrite").saveAsTable("ProductDescription")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Optimize Delta files

# CELL ********************

# MAGIC %%sql
# MAGIC OPTIMIZE SQLDB_Synapse.Product_Purchase_Consolidate

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Copy files from raw to Silver

# CELL ********************

##copy files


source="Files/Raw/SQLDB/Tables/Production.ProductDescription/"

destiny ='Files/Silver/'

folder_destination =  'SQLDB/Tables/Production.ProductDescription/'




def Copyfile(source,destiny,folder_destination):

    files = mssparkutils.fs.ls(source)

    for file in files:

        #print(file.name, file.isDir, file.isFile, file.path, file.size)

        mssparkutils.fs.cp(file.path,destiny+folder_destination+ file.name )




Copyfile (source,destiny,folder_destination)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Reading the table

# CELL ********************

df = spark.sql("SELECT * FROM SQLDB_Synapse.product_purchase_consolidate LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ### Copy to a local directory to use Pandas or With Open. Works!

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
import numpy as np
import timeit


##reading product from adventureworks exported into parquet mode
#mssparkutils.fs.cp('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/HumanResources.Employee/part-00000-0a3c54ce-93dd-4196-aaf4-7998a455c349-c000.snappy.parquet', 'file:/tmp/temp/HumanResources.Employee/part-00000-f88db24d-098f-4872-a525-2012fd9bc2ba-c000.snappy.parquet')
mssparkutils.fs.cp('abfss://NextSynapse12@msit-onelake.pbidedicated.windows.net/SQLDW.Lakehouse/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')

#pf = pd.read_parquet('file:/tmp/temp/HumanResources.Employee/part-00000-f88db24d-098f-4872-a525-2012fd9bc2ba-c000.snappy.parquet')
#pf.head()

#pyarrow works!
#pd.read_parquet('file:/tmp/temp/HumanResources.Employee/part-00000-f88db24d-098f-4872-a525-2012fd9bc2ba-c000.snappy.parquet', engine='pyarrow')

##fastparquet does not work


# mount the Blob Storage container, and then read the file by using a mount path
with open("/tmp/temp/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv") as f:
    print(f.read())

#https://techcommunity.microsoft.com/t5/azure-synapse-analytics-blog/essential-tips-for-exporting-and-cleaning-data-with-spark/ba-p/3779583

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Copying between workspaces ( or domains)

# CELL ********************

##reading product from adventureworks exported into parquet mode
mssparkutils.fs.cp('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/HumanResources.Department/part-00000-5a57e0aa-d02c-466e-baba-af33e77123ab-c000.snappy.parquet', 'abfss://NextSynapse12@msit-onelake.dfs.fabric.microsoft.com/datbricksOnelake.Lakehouse/Files/Silver//part-00000-5a57e0aa-d02c-466e-baba-af33e77123ab-c000.snappy.parquet')


df_Department = spark.read.parquet('abfss://NextSynapse12@msit-onelake.dfs.fabric.microsoft.com/datbricksOnelake.Lakehouse/Files/Silver//part-00000-5a57e0aa-d02c-466e-baba-af33e77123ab-c000.snappy.parquet')
df_Department.show(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
