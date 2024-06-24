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
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

 %%pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

#data_path = spark.read.load('abfss://filesystemdatalake@administrators.dfs.core.windows.net/FactInternetSales (1).parquet', format='parquet')
#https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/synapse-analytics/spark/synapse-file-mount-api.md
#display(data_path.limit(10))

#set variable to be used to connect the database
database = "AdventureWorks2017"
table = "information_schema.tables"
user = "testOwner"
password  = "Contoso!0000"
 

jdbcDF = spark.read \
        .format("jdbc") \
        .option("url",  f"jdbc:sqlserver://sqldbfta.database.windows.net:1433; database=AdventureWorks2017") \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password).load()


#Writetableread = spark.sql ("select * from AdventureWorks2017.HumanResources.EmployeeDepartmentHistory")

###I need to finish how to pass the variable here.
dataCollect = jdbcDF.collect()
for row in dataCollect:
    
    #nametable = spark.sparkContext.broadcast(row['TABLE_SCHEMA'] + "." +row['TABLE_NAME'])
    nametable = row['TABLE_SCHEMA'] + "." +row['TABLE_NAME']
    #print (nametable.value)
    print(nametable)#broadcast value #Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs.
    

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC from pyspark import SparkConf, SparkContext
# MAGIC from pyspark.sql import SparkSession
# MAGIC 
# MAGIC 
# MAGIC blob_account_name = "administrators"
# MAGIC blob_container_name = "filesystemdatalake"
# MAGIC 
# MAGIC sc = SparkSession.builder.getOrCreate()
# MAGIC token_library = sc._jvm.com.microsoft.azure.synapse.tokenlibrary.TokenLibrary
# MAGIC 
# MAGIC ###it seems SAS is not working.
# MAGIC spark.conf.set(
# MAGIC     'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
# MAGIC     '?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-02-26T22:41:34Z&st=2022-12-20T14:41:34Z&spr=https,http&sig=OJoW0MtG4qUENDmaaf1ehng9AoTpbK8WdoF1cKA5s7I%3D')
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #set variable to be used to connect the database
# MAGIC database = "AdventureWorks2017"
# MAGIC #table = "information_schema.tables"
# MAGIC table = "information_schema_tables" # I customize this table based on the information_schema.tables 
# MAGIC user = "testOwner"
# MAGIC password  = "Contoso!0000"
# MAGIC  
# MAGIC 
# MAGIC jdbcDF = spark.read \
# MAGIC         .format("jdbc") \
# MAGIC         .option("url",  f"jdbc:sqlserver://sqldbfta.database.windows.net:1433; database=AdventureWorks2017") \
# MAGIC         .option("dbtable", table) \
# MAGIC         .option("user", user) \
# MAGIC         .option("password", password).load()
# MAGIC 
# MAGIC 
# MAGIC #Writetableread = spark.sql ("select * from AdventureWorks2017.HumanResources.EmployeeDepartmentHistory")
# MAGIC 
# MAGIC dataCollect = jdbcDF.collect()
# MAGIC for row in dataCollect:
# MAGIC     
# MAGIC     #nametable = spark.sparkContext.broadcast(row['TABLE_SCHEMA'] + "." +row['TABLE_NAME'])
# MAGIC     nametable = spark.sparkContext.broadcast(row['tablename'])
# MAGIC     #nametable = row['TABLE_SCHEMA'] + "." +row['TABLE_NAME']
# MAGIC     #print (nametable)
# MAGIC     print(nametable.value)#broadcast value #Broadcast variables are read-only shared variables that are cached and available on all nodes in a cluster in-order to access or use by the tasks. Instead of sending this data along with every task, PySpark distributes broadcast variables to the workers using efficient broadcast algorithms to reduce communication costs.
# MAGIC     
# MAGIC     
# MAGIC     #set variable to be used to connect the database
# MAGIC     database = "AdventureWorks2017"
# MAGIC     table = nametable.value
# MAGIC     user = "testOwner"
# MAGIC     password  = "Contoso!0000"
# MAGIC     
# MAGIC     print(nametable.value )
# MAGIC 
# MAGIC     jdbcDF = spark.read \
# MAGIC             .format("jdbc") \
# MAGIC             .option("url",  f"jdbc:sqlserver://sqldbfta.database.windows.net:1433; database=AdventureWorks2017") \
# MAGIC             .option("dbtable", table) \
# MAGIC             .option("user", user) \
# MAGIC             .option("password", password).load()
# MAGIC 
# MAGIC     jdbcDF.write.mode("overwrite").parquet("Files/Files/SQLDB_next/Tables/" + nametable.value )

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************


pip install findspark
#https://sparkbyexamples.com/pyspark/how-to-import-pyspark-in-python-script/

# CELL ********************


import findspark
findspark.init()
#https://sparkbyexamples.com/pyspark/dynamic-way-of-doing-etl-through-pyspark/

