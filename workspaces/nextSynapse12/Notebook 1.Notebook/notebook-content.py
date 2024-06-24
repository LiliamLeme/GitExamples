# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc9fb29f-b1ad-448e-b32c-e0be6dd5f5c4",
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc9fb29f-b1ad-448e-b32c-e0be6dd5f5c4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

https://onelake.dfs.fabric.microsoft.com/NextSynapse12/SynapseFabric.Datawarehouse/Tables/dbo/bing_covid-19_data

# CELL ********************

df = spark.read.parquet("abfss://NextSynapse12@msit-onelake.dfs.fabric.microsoft.com/SynapseFabric.Datawarehouse/Tables/dbo/FactInternetSales_clone/F016EECD-D1A0-49C9-A47B-3C332989DE93")
# df now is a Spark DataFrame containing text data from "abfss://NextSynapse12@msit-onelake.dfs.fabric.microsoft.com/datbricksOnelake.Lakehouse/Files/Silver/Log/App3.log.txt".
display(df)




# CELL ********************

# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.

df = spark.sql("SELECT * FROM [SynapseFabric].[dbo].[bing_covid-19_data] LIMIT 1000")
display(df)

# CELL ********************


# With Spark SQL, Please run the query onto the lakehouse which is from the same workspace as the current default lakehouse.

df = spark.sql("SELECT * FROM datbricksOnelake.FactInternetSales_Consold LIMIT 1000")
display(df)
