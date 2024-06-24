# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "42cf0ef9-bae2-4cc0-9207-83e678465351",
# META       "default_lakehouse_name": "LH_LiLem",
# META       "default_lakehouse_workspace_id": "8f08155f-f0fb-4821-8ba2-e333af1bff7f",
# META       "known_lakehouses": [
# META         {
# META           "id": "42cf0ef9-bae2-4cc0-9207-83e678465351"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.parquet("Files/sample_datasets/nyc_taxi_green_2020_11.parquet")
# df now is a Spark DataFrame containing parquet data from "Files/sample_datasets/nyc_taxi_green_2020_11.parquet".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
