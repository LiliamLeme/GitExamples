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

from datetime import datetime, timedelta

print("Starting...")
delay = timedelta(seconds=300) ##5 min
endtime = datetime.now() + delay
while datetime.now() < endtime:
    pass
print("...Finished")

##time.sleep(3)  # Pause for 3 seconds

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sparkContext._conf.getAll()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.network.timeout", "600s").getOrCreate()

# Your Spark application logic goes here

# Stop SparkSession when done
#spark.stop()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pip install pyspark


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
