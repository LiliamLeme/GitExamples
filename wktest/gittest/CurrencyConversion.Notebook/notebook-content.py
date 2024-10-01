# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "30489185-60ec-4166-9f67-8df9d352b1af",
# META       "default_lakehouse_name": "testdata",
# META       "default_lakehouse_workspace_id": "45ed8171-d058-4807-b868-42c1ac3d3199"
# META     }
# META   }
# META }

# CELL ********************

##Libraries
import requests
import json
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



response = requests.get("https://open.er-api.com/v6/latest/USD")
byte_string  = response.content
json_string = byte_string.decode('utf-8')
data = json.loads(json_string)
json_output = json.dumps(data, indent=4)

# Print or use the JSON data
print(json_output)
#response_json = json.dumps(response.content)  

# Print the status code of the response.
print(response.status_code)
#print(response.content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


data = json.loads(json_output)


flattened_data = pd.json_normalize(data)


df = pd.DataFrame(flattened_data)


print(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

##Libraries
import requests
import json
import pandas as pd
from pyspark.sql.functions import col

##class to get the live currency from free API. 
base_url: str = "https://open.er-api.com/v6/latest"
class LiveCurrency:
    def __init__(
        self
    ) -> None:
        self.base_url = base_url
    def LiveCurrency_base(self, base: str) -> str:
        """Method def -
            Execute open API https://open.er-api.com/v6/latest using as parameter the currency
         Args:
             base (str): USD for dollar, GBP for Pounds
        Returns:
        Code response Success message
                       Fail message
        """
        try:
            url = (
                f"{self.base_url}/{base}"
            )
            #print (url)
            response = requests.get(url, timeout=120)
            ##remove the decode 'b if sucessfull
            if response.status_code==200:
                byte_string  = response.content
                json_string = byte_string.decode('utf-8')
                data = json.loads(json_string)
                json_output = json.dumps(data, indent=4)
                return json_output
            return response.raise_for_status
        except requests.exceptions.RequestException as req_err:
            return(f"Request error occurred: {req_err}")
    def LiveCurrency_table(self, base: str, table_base: str, append_overwrite: str) -> str:
        """Method def -
            Save live currency as table 
         Args:
             base (str): USD for dollar, GBP for Pounds
        Returns:
        Code response Success message
                       Fail message
        """
        try:
            ##flatten the output and save as table
            json_output= self.LiveCurrency_base(base)
            data = json.loads(json_output)
            flattened_data = pd.json_normalize(data)
            df = pd.DataFrame(flattened_data)
            
            ##making sure the column is using double as datatype and the name is only rates not rate.currency
            df = df.apply(lambda x: x.astype(float) if x.dtype == 'int64' else x)
            original_column_name = f"rates.{base}"
            df.rename(columns={original_column_name: 'rates'}, inplace=True)  
            spark_df = spark.createDataFrame(df)
            spark_df.write.format("delta").mode(append_overwrite).option("mergeSchema", "true").saveAsTable(table_base)

            return(f"{table_base} Created")
        except requests.exceptions.RequestException as req_err:
            return(f"Request error occurred: {req_err}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client = LiveCurrency()

#response = client.LiveCurrency_base("USD")
#response
response = client.LiveCurrency_table("USD", "Live_Currency", "append")##_overwrite"
response 
response = client.LiveCurrency_table("GBP", "Live_Currency", "append")
response 
response = client.LiveCurrency_table("KYD", "Live_Currency", "append")
response 
response = client.LiveCurrency_table("JOD", "Live_Currency", "append")
response 
response = client.LiveCurrency_table("CHF", "Live_Currency", "append")
response 
response = client.LiveCurrency_table("EUR", "Live_Currency", "append")
response 
response = client.LiveCurrency_table("JPY", "Live_Currency", "append")
response 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM testdata.livecurrency_table LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


client = LiveCurrency()

#response = client.LiveCurrency_base("USD")
#response
response = client.LiveCurrency_table("USD")
response ## management api scope

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
