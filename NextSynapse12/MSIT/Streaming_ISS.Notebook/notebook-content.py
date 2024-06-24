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

# MARKDOWN ********************

# #### Trying to send the data to event stream in Fabric

# CELL ********************

pip install azure-eventhub


# CELL ********************

pip install apscheduler

# CELL ********************

import requests

# Make a get request to get the latest position of the international space station from the opennotify api.
response = requests.get("http://api.open-notify.org/iss-now.json")

# Print the status code of the response.
print(response.status_code)
print(response.content)

##https://briankolowitz.github.io/data-focused-python/lectures/Topic%2008%20-%20Making%20Web%20Requests/02%20-%20Getting%20Data%20from%20Web%20APIs.html

# MARKDOWN ********************

# #### Getting from API and send it to event stream in Fabric ( this one does not use Spark Stream just Event Stream)

# CELL ********************

import requests
from azure.eventhub import EventHubProducerClient, EventData
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import json

# Azure Event Hubs configuration
eventhub_namespace = "esehwestca49o86gbiyxad7s" ##es_spark_test
eventhub_name = "es_b6aab53d-a0a8-4af4-ad24-0f7863048c96"
sas_key_name = "key_3b55aeb8-1655-9691-0731-63e1c7d5d323"
sas_key_value = "EmCRpSQBAkPbG1VegqlsLI3eJq+QXBLEq+AEhFC/Zg0"

# Function to get ISS data and send it to Azure Event Hub
def send_iss_data():
    try:
        # Make a GET request to get the latest position of the ISS from the opennotify API
        response = requests.get("http://api.open-notify.org/iss-now.json")

        if response.status_code == 200:
            
            iss_data = response.json()

            # Convert iss_data to JSON string
            iss_data_json = json.dumps(iss_data)

            # Send data to Azure Event Hub
            eventhub_connection_str = (
                f"Endpoint=sb://esehwestca49o86gbiyxad7s.servicebus.windows.net/;SharedAccessKeyName=key_3b55aeb8-1655-9691-0731-63e1c7d5d323;SharedAccessKey=EmCRpSQBAkPbG1VegqlsLI3eJq+QXBLEq+AEhFC/Zg0=;EntityPath=es_b6aab53d-a0a8-4af4-ad24-0f7863048c96"
            )
            producer = EventHubProducerClient.from_connection_string(eventhub_connection_str)

            with producer:
                event_data_batch = producer.create_batch()

                # Add the event data to the batch
                event_data_batch.add(EventData(body=iss_data_json))

                # Send the batch to the event hub
                producer.send_batch(event_data_batch)

            print("Data sent to Azure Event Hub successfully.")
        else:
            print(f"Failed to retrieve ISS data. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error: {str(e)}")

###https://learn.microsoft.com/en-us/fabric/real-time-analytics/event-streams/overview#various-source-connectors

# Set up a scheduler to run the function every 5 seconds
scheduler = BlockingScheduler()
scheduler.add_job(send_iss_data, 'interval', seconds=5)

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    print("Scheduler stopped.")

###https://learn.microsoft.com/en-us/fabric/real-time-analytics/event-streams/overview#various-source-connectors


# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.ISS_Stream_Fabric LIMIT 1000")
display(df)

# MARKDOWN ********************

# ### Creating json files from the API to  Streaming

# CELL ********************

import requests
import time
import json

# Define the API endpoint
api_endpoint = "http://api.open-notify.org/iss-now.json"

# Set the number of requests
num_requests = 10  # Change this to the desired number of requests

for i in range(num_requests):
    # Make a get request to get the latest position of the international space station from the opennotify API.
    response = requests.get(api_endpoint)

    # Print the status code of the response.
    print(f"Request {i+1} - Status Code: {response.status_code}")

    # Save the response content to a file
    filename = f"iss_data_{i+1}.json"

    filepath =  '/lakehouse/default/Files/Stream/' +filename
    with open(filepath, "w") as file:
        file.write(response.text)

    # Wait for 3 seconds before making the next request
    time.sleep(3)

    # Print a separator line between requests
    print("-" * 30)


# MARKDOWN ********************

# ### Using Spark Streaming to get the data into the Lakehouse.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import window
import time

#spark = SparkSession \
#.builder \
#.appName("Streaming Session") \
#.getOrCreate()

# Define the schema for the JSON data
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("iss_position", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True)
    ]), True)
])

# Specify the file path or wildcard pattern
inputPath = 'Files/Stream'
checkpoint_path = "Files/Stream/checkpoint"

TableName = "ISS_NB_Stream"
deltaTablePath = "Tables/" + TableName

streamingInputDF = (
    spark
    .readStream
    .format('json')
    .schema(schema)
    .load(inputPath)
)

##it does not work
#query = (
#  streamingInputDF
#    .writeStream
#    .format("delta")        # memory = store in-memory table (for testing only)
#    .option("checkpointLocation", "Files/Stream/checkpoint")
#    .outputMode("append")
#    .trigger(processingTime="1 minute")
#    .toTable("ISS_NB_Stream")
    ##.awaitTermination()##start()
#)

query = (
  streamingInputDF
    .writeStream
    .format("delta")        # memory = store in-memory table (for testing only)
    .outputMode("append")
    .option("path", deltaTablePath)
    .option("checkpointLocation", deltaTablePath + "/checkpoint")
    .start()
)

#query.awaitTermination()
#query.awaitTermination()

time.sleep(15)
query.stop()

##https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-streaming-data#streaming-sources
##https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-streaming-data#streaming-sources



# MARKDOWN ********************

# ### Another example of streaming

# CELL ********************

import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

inputPath = 'Files/Stream'
checkpoint_path = "Files/Stream/checkpoint"

TableName = "streamingtable"
deltaTablePath = "Tables/" + TableName

df = spark.readStream.format("rate").option("rowsPerSecond", 1).load(inputPath)

query = df.writeStream.outputMode("append").format("delta").option("path", deltaTablePath).option("checkpointLocation", deltaTablePath + "/checkpoint").start()
query.awaitTermination()

# MARKDOWN ********************

# ### Checking the postion on the map through the lakehouse

# CELL ********************

pip install folium

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
import pandas as pd

# Read the Delta table
iss_positions_spark = spark.sql("SELECT * FROM SQLDW.ISS_Stream_Fabric LIMIT 1000")

# Show the DataFrame schema and data
iss_positions_spark.printSchema()
iss_positions_spark.show(truncate=False)

# Convert the timestamp to a human-readable format
iss_positions_spark = iss_positions_spark.withColumn("timestamp", from_unixtime(col("timestamp")))

# Extract latitude, longitude, and timestamp from the JSON column
iss_positions_spark = iss_positions_spark.withColumn("latitude", col("iss_position.latitude").cast("double"))
iss_positions_spark = iss_positions_spark.withColumn("longitude", col("iss_position.longitude").cast("double"))

# Select relevant columns
iss_position_df = iss_positions_spark.select("latitude", "longitude", "timestamp")

# Convert to Pandas DataFrame
iss_position_df_pandas = iss_position_df.toPandas()

# Show the Pandas DataFrame
print(iss_position_df_pandas)


# CELL ********************

import folium
import matplotlib.pyplot as plt
import pandas as pd


# Create a map using folium
mymap = folium.Map(location=[0, 0], zoom_start=2)

# Plot ISS positions on the map
for index, row in iss_position_df_pandas.iterrows():
    folium.Marker([float(row['latitude']), float(row['longitude'])], popup=row['timestamp']).add_to(mymap)

# Save the map as an HTML file
mymap.save('iss_map.html')

# Alternatively, you can use matplotlib to create a scatterplot
plt.scatter(iss_position_df_pandas['longitude'], iss_position_df_pandas['latitude'])
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.title('ISS Positions Over Time')
plt.show()


# MARKDOWN ********************

# ### Cleanup and check up of the tables

# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.ISS_NB_Stream LIMIT 1000")
display(df)
##df = spark.sql("DROP TABLE  SQLDW.ISS_NB_Stream ")
