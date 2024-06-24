# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc9fb29f-b1ad-448e-b32c-e0be6dd5f5c4",
# META       "default_lakehouse_name": "datbricksOnelake",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
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


# CELL ********************

df = spark.sql("SELECT * FROM datbricksOnelake.FactInternetSales_Consold LIMIT 1000")
display(df)
