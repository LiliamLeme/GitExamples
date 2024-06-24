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

# CELL ********************

import org.apache.spark.SparkConf

val conf = new SparkConf()
  .set("spark.logAnalytics.enabled", "true")
  .set("spark.logAnalytics.workspaceId", "ea81d5a5-d812-4ff3-93ad-939bd121f175")
  .set("spark.logAnalytics.directoryId", "<Your-Directory-ID>")
  .set("spark.logAnalytics.applicationId", "<Your-Application-ID>")
  .set("spark.logAnalytics.aadTenantId", "<Your-AAD-Tenant-ID>")



# CELL ********************

# MAGIC %%spark
# MAGIC import org.apache.spark.SparkConf
# MAGIC 
# MAGIC val conf = new SparkConf()
# MAGIC   .set("spark.logAnalytics.enabled", "true")
# MAGIC   .set("spark.logAnalytics.workspaceId", "ea81d5a5-d812-4ff3-93ad-939bd121f175")
# MAGIC   .set("spark.logAnalytics.secret",  "fu/JZus/MVfN4jzg92L4GasKRqmmGP7UBhloPaAknOQ9FwwvEsHAqPm0i9zt5PLJYGv6E+OVHNHraOZxpE/bGQ==")###FP98Q~z.cp0c8ukLSC1oTdIF4jF21KTxZYc5taFY
# MAGIC   .set("spark.logAnalytics.aadTenantId", "72f988bf-86f1-41af-91ab-2d7cd011db47")


# METADATA ********************

# META {
# META   "language": "scala"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC logger = sc._jvm.org.apache.log4j.LogManager.getLogger("com.contoso.PythonLoggerExample")
# MAGIC logger.info("info message")
# MAGIC logger.warn("warn message")
# MAGIC logger.error("error message")

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

pip install requests


# CELL ********************

import requests
import json


# CELL ********************

workspace_id = "ea81d5a5-d812-4ff3-93ad-939bd121f175"
primary_key = "fu/JZus/MVfN4jzg92L4GasKRqmmGP7UBhloPaAknOQ9FwwvEsHAqPm0i9zt5PLJYGv6E+OVHNHraOZxpE/bGQ=="
api_version = "2016-04-01"
log_type = "samplefabric_CL"  # Replace with the desired log type
log_data = {}  # Your log data in JSON format

##https://learn.microsoft.com/en-us/answers/questions/1154380/where-is-azure-is-the-primary-key-and-workspace-id
#https://www.systanddeploy.com/2022/05/starting-with-log-analytics-part-2.html
#https://learn.microsoft.com/en-us/rest/api/loganalytics/create-request


# CELL ********************

log_data = {
    "EventTime": "2023-09-25T10:30:45Z",
    "EventType": "SystemEvent",
    "EventID": 12345,
    "Severity": "Information",
    "Message": "System event message goes here.",
    "Source": "MyApplication",
    "IPAddress": "192.168.1.100",
    "User": "JohnDoe",
    "AdditionalInfo": {
        "Key1": "Value1",
        "Key2": "Value2",
        "Key3": "Value3"
    }
}


# CELL ********************

import requests
resp = requests.get('https://reqres.in/api/users')
resp_dict = resp.json()

print(type(resp_dict))

##https://datagy.io/python-requests-json/

##https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal#generate-sample-data

##https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-overview

##https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal#sample-data


# MARKDOWN ********************

# https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-query-readme?view=azure-python
# 
# https://learn.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api?tabs=powershell
# 
# https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-azure-log-analytics
# 
# https://learn.microsoft.com/en-us/azure/architecture/databricks-monitoring/application-logs#send-application-logs-using-log4j
# 
# 
# https://learn.microsoft.com/en-us/power-bi/transform-model/log-analytics/desktop-log-analytics-configure
# 
# 
# https://docs.python-requests.org/en/latest/api/


# CELL ********************

import requests
import json


workspace_id = "ea81d5a5-d812-4ff3-93ad-939bd121f175"
primary_key = "fu/JZus/MVfN4jzg92L4GasKRqmmGP7UBhloPaAknOQ9FwwvEsHAqPm0i9zt5PLJYGv6E+OVHNHraOZxpE/bGQ=="
api_version = "2016-04-01"
log_type = "samplefabric_CL"  # Replace with the desired log type
log_data = {}  # Your log data in JSON format



base_url = f"https://ea81d5a5-d812-4ff3-93ad-939bd121f175.ods.opinsights.azure.com"
api_path = f"/api/logs?api-version={api_version}"

url = 'https://ea81d5a5-d812-4ff3-93ad-939bd121f175.ods.opinsights.azure.com/RestApi.svc/api/logs'
#base_url + api_path
print (url)


#base_url = f"https://api.loganalytics.io/v1/workspaces/ea81d5a5-d812-4ff3-93ad-939bd121f175/query"


api_path = f"/api/logs?api-version={api_version}"

url = base_url + api_path


headers = {
    "Authorization": f"SharedKey {workspace_id}:{primary_key}",
    "Log-Type": log_type,
    "Content-Type": "application/json"
}
##https://learn.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api?tabs=powershell
##https://github.com/TheMaoz/GetYourDataOut/blob/main/GetYourDataOut.ipynb

response = requests.post(url, headers=headers, data=json.dumps(log_data))

if response.status_code == 200:
    print("Data sent successfully to Azure Log Analytics.")
else:
    print(f"Failed to send data. Status code: {response.status_code}")




# MARKDOWN ********************

# tried access the log analytics with SP giving permissions to the API with 
# https://entra.microsoft.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/CallAnAPI/appId/985134f5-4ec8-4d61-aa9d-6b0325e98ec9/isMSAApp~/false
# 
# https://learn.microsoft.com/en-us/azure/azure-monitor/logs/api/access-api
# 


# CELL ********************

import requests
import json
import adal
import time
import hashlib
import hmac
import base64

# Replace with your Azure AD and Log Analytics information
tenant_id = '72f988bf-86f1-41af-91ab-2d7cd011db47'
client_id = '985134f5-4ec8-4d61-aa9d-6b0325e98ec9'
client_secret = 'eMd8Q~Sxw9Xjc.Xdi-zsX9xThrq4SWJkHwdP7ba-'
workspace_id = 'ea81d5a5-d812-4ff3-93ad-939bd121f175'
log_type = 'YOUR_LOG_TYPE'
##https://entra.microsoft.com/#view/Microsoft_AAD_IAM/TenantOverview.ReactView

# Construct the authorization token
authority_url = f'https://login.microsoftonline.com/{tenant_id}'
context = adal.AuthenticationContext(authority_url)
token = context.acquire_token_with_client_credentials(resource=workspace_id, client_id=client_id, client_secret=client_secret)


# Generate the current timestamp in RFC 7231 format
timestamp = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime())

# Generate a unique GUID for the request ID
request_id = str(uuid.uuid4())

# The data you want to send to Log Analytics
data = {
    'your_field1': 'value1',
    'your_field2': 'value2'
}

# Convert the data to JSON format
json_data = json.dumps(data)

# Create a hash of the data
sha256_hash = hashlib.sha256(json_data.encode()).digest()

# Create a HMAC-SHA256 signature
signature = base64.b64encode(hmac.new(base64.b64decode(token['access_token']), sha256_hash, digestmod=hashlib.sha256).digest()).decode()

# Send the data to Log Analytics
url = f'https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version=2016-04-01'
headers = {
    'Authorization': f'Bearer {token["access_token"]}',
    'Log-Type': log_type,
    'x-ms-date': timestamp,
    'time-generated-field': '',
    'request-id': request_id,
    'Content-Type': 'application/json',
}
response = requests.post(url, data=json_data, headers=headers)

if response.status_code == 200:
    print('Data sent successfully to Log Analytics.')
else:
    print(f'Failed to send data. Status Code: {response.status_code}, Response Content: {response.content.decode()}')

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import json

##reading product from adventureworks exported into parquet mode
#mssparkutils.fs.cp('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/HumanResources.Employee/part-00000-0a3c54ce-93dd-4196-aaf4-7998a455c349-c000.snappy.parquet', 'file:/tmp/temp/HumanResources.Employee/part-00000-f88db24d-098f-4872-a525-2012fd9bc2ba-c000.snappy.parquet')
mssparkutils.fs.cp('Files/JsonHack/samplelog.log', 'file:/tmp/JsonHack/samplelog.log')

logfile= "/tmp/JsonHack/samplelog.log"

#with open("/tmp/JsonHack/samplelog.log", "r") as log_file:
#    log_string = log_file.read()
#response_string = log_string.split("Response:")[1].strip()

#response_obj = json.loads(response_string)

#with open("/Files/JsonHack/outfile", "w") as out_file:
#    out_file.write(json.dumps(response_obj))

    ##https://docs.python.org/2/library/json.html
    #https://stackoverflow.com/questions/49730175/log-file-read-and-convert-to-json



a = open(logfile,'r')
text = a.read()
text_as_list = text.split('\n')
keys = text_as_list[2].split()
result = []
for item in text.split('\n')[4:len(text_as_list)]:
    temp_dict = {}
for i,j in zip(keys,item.split()):  
    if j.isdigit():         
        temp_dict[i] = int(j)
    else:
        temp_dict[i] = j
result.append(temp_dict)
print (json.dumps(result))

with open("/lakehouse/default/Files/JsonHack/jsonsample.json", "w") as out_file:
    out_file.write(json.dumps(result))


# CELL ********************

import json

# Replace 'your_log_file.txt' with the path to your log file
log_file_path = '/lakehouse/default/Files/JsonHack/samplelog.log'

# List to store the parsed log entries as JSON objects
log_entries = []

# Read the log file line by line
with open(log_file_path, 'r') as log_file:
    for line in log_file:
        # Split the line into its components based on whitespace (assuming log format consistency)
        components = line.split()

        # Create a dictionary for the log entry
        log_entry = {
            "ip": components[0],
            "user": components[1],
            "timestamp": components[3] + ' ' + components[4],
            "request": components[5] + ' ' + components[6] + ' ' + components[7],
            "status_code": int(components[8]),
            "response_size": int(components[9]),
            "referrer": components[10],
            "user_agent": ' '.join(components[11:]).strip(),
            "extra_info": "-"
        }

        # Append the log entry to the list
        log_entries.append(log_entry)

# Convert the list of log entries to a JSON string
log_entries_json = json.dumps(log_entries, indent=2)

# Print or save the JSON data as needed
print(log_entries_json)

with open("/lakehouse/default/Files/JsonHack/jsonsample_new.json", "w") as out_file:
    out_file.write(json.dumps(log_entries, indent=2))



# CELL ********************

from notebookutils import mssparkutils 
from trident_token_library_wrapper import PyTridentTokenLibrary
# get access token for keyvault resource
# you can also use full audience here like https://vault.azure.net
access_token = PyTridentTokenLibrary.get_access_token("keyvault") # The "keyvault" is a hard coded resource 
id, you don't need to change it
accountKey = PyTridentTokenLibrary.get_secret_with_token("<vaultURI>", "<secretName>", access_token)
mssparkutils.fs.mount( 
 "abfss://mycontainer@<accountname>.dfs.core.windows.net", 
 "/test", 
 {"accountKey":accountKey}

