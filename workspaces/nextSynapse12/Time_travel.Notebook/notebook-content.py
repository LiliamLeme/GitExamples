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
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### Whoops wrong update done

# CELL ********************

# MAGIC %%pyspark
# MAGIC ##Whoops Data was changed accidentaly
# MAGIC 
# MAGIC from delta.tables import *
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, "abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Tables/productdescription")
# MAGIC  
# MAGIC 
# MAGIC # Update the table (reduce price of accessories by 10%)
# MAGIC deltaTable.update(
# MAGIC     condition = "ModifiedDate == '2013-04-30 00:00:00.000'",
# MAGIC     set = { "ModifiedDate": "'2021-04-30 00:00:00.000'" }
# MAGIC                 )

# METADATA ********************

# META {
# META   "language": "python"
# META }

# MARKDOWN ********************

# #### Reading the deltatable history to find the change

# CELL ********************

# MAGIC %%pyspark
# MAGIC ##Which Version was?
# MAGIC #//listingversions - Find the version
# MAGIC from delta.tables import *
# MAGIC deltaTable = DeltaTable.forPath(spark, "abfss://NextSynapse12@msit-onelake.dfs.fabric.microsoft.com/SynapseFabric.Datawarehouse/Tables/dbo/FactInternetSales_clone_retorepoint")
# MAGIC latestHistory = deltaTable.history(); 
# MAGIC latestHistory.show(100) 
# MAGIC 
# MAGIC ##C:\Users\lilem\OneLake - Microsoft\NextSynapse12\SynapseFabric.Datawarehouse
# MAGIC 


# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

1+1 

# CELL ********************

# MAGIC %%pyspark
# MAGIC ##Which Version was?
# MAGIC #//listingversions - Find the version
# MAGIC from delta.tables import *
# MAGIC deltaTable = DeltaTable.forPath(spark, "abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Tables/productdescription")
# MAGIC latestHistory = deltaTable.history(); 
# MAGIC latestHistory.show(100) 

# METADATA ********************

# META {
# META   "language": "python"
# META }

# MARKDOWN ********************

# #### Here we have the many rows that change and also prior the change. 
# ( it should return 636 rows)
# #### Once we confirm if we should restore to the previous state, I can overwrite the table results with the timetravel filter

# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC //Timetravel. Get back on time. 
# MAGIC //Scala
# MAGIC val df_read_timetravel = spark.read.format("delta").option("timestampAsOf", "2023-06-14 14:25").load("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Tables/productdescription")
# MAGIC df_read_timetravel.createOrReplaceTempView("view_timetravel")
# MAGIC val view_timetravel = spark.sqlContext.sql ("select count(*)  from view_timetravel WHERE ModifiedDate = '2013-04-30 00:00:00.000'");
# MAGIC view_timetravel.show()
# MAGIC 
# MAGIC val df_read_current = spark.read.format("delta").load("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Tables/productdescription")
# MAGIC df_read_current.createOrReplaceTempView("view_current")
# MAGIC val df_current = spark.sqlContext.sql ("select count(*) from view_current WHERE ModifiedDate = '2021-04-30 00:00:00.000'");
# MAGIC df_current.show()
# MAGIC 
# MAGIC //save the files from a point in time.
# MAGIC df_read_timetravel.write.mode("overwrite").format("delta").save("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Tables/productdescription")

# METADATA ********************

# META {
# META   "language": "scala"
# META }

# MARKDOWN ********************

# #### Confirming the status of the actual data. Once it was overwriten the information with the incorrect filter should not be there anymore
# it should return 0

# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC //Timetravel. Get back on time. 
# MAGIC //Scala
# MAGIC val df_read_current = spark.read.format("delta").load("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Tables/productdescription")
# MAGIC df_read_current.createOrReplaceTempView("view_current")
# MAGIC val df_current = spark.sqlContext.sql ("select count(*) from view_current WHERE ModifiedDate = '2021-04-30 00:00:00.000'");
# MAGIC df_current.show()


# METADATA ********************

# META {
# META   "language": "scala"
# META }
