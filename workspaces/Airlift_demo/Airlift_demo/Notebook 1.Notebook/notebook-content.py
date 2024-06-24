# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ddd3c697-3976-4bcc-87e6-94b8babfcbc6",
# META       "default_lakehouse_name": "Airlift_LH",
# META       "default_lakehouse_workspace_id": "44e31f04-34e7-4ac8-909a-2663460eea4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "ddd3c697-3976-4bcc-87e6-94b8babfcbc6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# CELL ********************

df = spark.sql("SELECT * FROM Airlift_LH.FactInternetSales_new LIMIT 1000")
display(df)
