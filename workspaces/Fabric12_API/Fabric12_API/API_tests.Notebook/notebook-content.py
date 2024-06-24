# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "315c2cd0-113d-4f5f-8762-fe947beb5ee5",
# META       "default_lakehouse_name": "leme_api_test",
# META       "default_lakehouse_workspace_id": "36f9bb12-d7c7-4c67-8aa2-746a7b4c4c34"
# META     }
# META   }
# META }

# CELL ********************

secret LEz8Q~JSnSrPOoObHrClv~2ZqBfmonJc20HHgcqm
app 985134f5-4ec8-4d61-aa9d-6b0325e98ec9
tenant 72f988bf-86f1-41af-91ab-2d7cd011db47
nome app_fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = "72f988bf-86f1-41af-91ab-2d7cd011db47"
client_id = "985134f5-4ec8-4d61-aa9d-6b0325e98ec9"
client_secret = "LEz8Q~JSnSrPOoObHrClv~2ZqBfmonJc20HHgcqm" 

resource = 'https://api.fabric.microsoft.com'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
from msal import ConfidentialClientApplication



# Acquire token using MSAL
authority = f'https://login.microsoftonline.com/{tenant_id}'
app = ConfidentialClientApplication(
    client_id = client_id,
    authority=authority,
    client_credential=client_secret
)

token_response = app.acquire_token_for_client(scopes=[f'{resource}/.default'])
access_token = token_response['access_token']


# Make request to Microsoft Graph API to get service principals
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

headers


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from urllib.parse import urlparse

# Define the URL
url = "https://api.fabric.microsoft.com/v1/workspaces/"


jsonPayload = {
    "displayName": "lemedemo_test123"
}



# Make the POST request
response = requests.post(url,json=jsonPayload,  headers=headers)
# Check for HTTP errors
response.raise_for_status()
print (response)

if response.status_code == 200 or response.status_code == 201:
    print("Request accepted. Processing in progress.")

    
    location_header = response.headers.get("Location")
    parsed_url = urlparse(location_header)
    path_parts = parsed_url.path.split('/')
    workspace_id = path_parts[-1] 

    print (location_header)
    print(workspace_id)

    #Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
    url_get= f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
    #response_status = requests.get(url_get, headers=headers)
    # Check the response. If 202, the pipeline job was queue
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
