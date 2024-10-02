# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c557e0a3-13eb-43df-8750-bfbf032e663c",
# META       "default_lakehouse_name": "LKA",
# META       "default_lakehouse_workspace_id": "d21dfb2b-a460-4822-8724-39cef0dc18f2"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Variables

# MARKDOWN ********************

# #### With minimum permissions

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = ""
client_id = ""
client_secret = "" 

resource = 'https://api.fabric.microsoft.com'



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### get credential token

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

headers = {
    'Authorization': f'Bearer <copy and paste F12>',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}


headers

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### List workspace

# CELL ********************

 

url_get= f"https://api.fabric.microsoft.com/v1/workspaces/"
response_status = requests.get(url_get, headers=headers)
response_data = response_status.json()
status = response_data.get('status')

print (response_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create workspace

# CELL ********************

from urllib.parse import urlparse

# Define the URL
url = "https://api.fabric.microsoft.com/v1/workspaces/"


jsonPayload = {
    "displayName": "WORKSPACE_NAME",
    "capacityId" : "ID - Find it in the Admin portal - Capacities" 
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

    url_get= f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"





   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Drop Workspace

# CELL ********************

import requests

workspace_id =  ""

url_del = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"

response = requests.delete(url_del, headers=headers)

if response.status_code == 200:
    print("DELETE request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create Pipeline

# CELL ********************


import requests

workspace_id = ""


url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines"

payload = {
    "displayName": "Create_Pipl_API2",
    "description": "A data pipeline created  by Rest API"
}

# Make the POST request
response = requests.post(url_pipd,json=payload,  headers=headers)
# Check for HTTP errors


print (response)

response.raise_for_status()
if response.status_code in (201,202):
    print("Post request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Delete Pipeline

# CELL ********************


import requests

workspace_id = ""
pipelineid= ""


url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines/{pipelineid}"


response = requests.delete(url_pipd, headers=headers)

if response.status_code == 200:
    print("DELETE request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Executing Data PIpelines

# CELL ********************


import requests

workspace_id = ""
pipelineid= ""


url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines/{pipelineid}"
payload = {"executionData":{}}

response = requests.post(url_pipd, json = payload, headers=headers)
print (response)

if response.status_code in(200,201,202):
    print("Execute request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Lakehouse

# MARKDOWN ********************

# #### Create

# CELL ********************


import requests

workspace_id = ""


url_lh = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"

payload = {
    "displayName": "LH_API",
    "description": "A Lakehouse created  by Rest API"
}

# Make the POST request
response = requests.post(url_lh,json=payload,  headers=headers)
# Check for HTTP errors
response.raise_for_status()

print (response)

if response.status_code in (201,202):
    print("Post request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Notebook


# MARKDOWN ********************

# #### Create

# CELL ********************


import requests

workspace_id = ""


url_nb = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"

payload = {
    "displayName": "Notebook_API1",
    "description": "A Notebook created  by Rest API"
}

# Make the POST request
response = requests.post(url_nb,json=payload,  headers=headers)
# Check for HTTP errors
response.raise_for_status()

print (response)

if response.status_code in (201,202):
    print("Post request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drop

# CELL ********************


import requests

workspace_id = ""
NBID = ""

url_nb = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks/{NBID}"

# Make the POST request
response = requests.delete(url_nb,  headers=headers)
# Check for HTTP errors
response.raise_for_status()

print (response)

if response.status_code in (201,202):
    print("Delete request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
