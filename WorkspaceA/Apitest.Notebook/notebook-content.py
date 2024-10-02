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

# CELL ********************

gKu8Q~_4gv_Nx0bGs-DnIowRx1UybjIY3ewkhb-Y

app 908e6e3d-deed-4ef0-9a04-cc30bad96e7d
tenant 3c863c9b-2221-4236-88c3-37fe9e1d06f8
nome API_fabric


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Display name:new_api_fabric
Application (client) ID:a49b035c-f602-4b26-be68-784fb7c374c9
Object ID:f44c41e5-0778-4d84-a15a-163bd6ca1f50
Directory (tenant) ID:3c863c9b-2221-4236-88c3-37fe9e1d06f8
gKu8Q~_4gv_Nx0bGs-DnIowRx1UybjIY3ewkhb-Y

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Variables

# MARKDOWN ********************

# #### Permission works

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
client_secret = "yk58Q~Wf2G1yGMxkPw6jFLwu-1no1zGlKp3OkcF1" 

resource = 'https://management.azure.com'
##endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import msal
import requests
from azure.identity import DefaultAzureCredential
from requests.adapters import HTTPAdapter



def get_auth_token_SP(key_vault_name: str, client_id: str, scope: str) -> str:
    """
    Gets the auth token for ROPC grant type and the Fabric API scopes
    Arguments:
        key_vault_name (str): Key Vault Name where to get secrets - Tenant Id,
                              Application  Id, Fabric Secret
        clientId(str): client ID of a user-assigned managed identity to connect to the key vault.
        scope: https://management.azure.com
    Returns:
        str: Access Token
    """
    tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
    application_id = "c52eafb7-7387-466e-ad41-98164a73256a"
    fabric_secret = "yk58Q~Wf2G1yGMxkPw6jFLwu-1no1zGlKp3OkcF1" 
    
    scope = scope

    result = None

    authority = f'https://login.microsoftonline.com/{tenant_id}'
    app = msal.ConfidentialClientApplication(
        client_id = application_id,
        authority=authority,
        client_credential=fabric_secret)
    try:
        result = app.acquire_token_for_client(
        scopes=[f'{scope}/.default'])
        

        if "access_token" in result:
            access_token: str = result["access_token"]
            print("Successfully got the access token")
        else:
            if "error" in result:
                error = result.get("error_description") or result.get("error")
                error_message = (
                    "Error acquiring token for "
                    f"Application Id - Service Principal {client_id}:"
                    f"{error}"
                )
                print(error_message)
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred in create the capacity: {req_err}")
    return access_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_auth_token_SP("aa", "aa","https://management.azure.com")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### With minimum permissions

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "1382bbf1-6d66-48b3-8374-b6b7173211e7"
client_secret = "Use8Q~przbDyulOCfaIPG6U2tOPO.bjGvtrq_a3M" 

resource = 'https://api.fabric.microsoft.com'
##endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'


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
    'Authorization': f'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IktRMnRBY3JFN2xCYVZWR0JtYzVGb2JnZEpvNCIsImtpZCI6IktRMnRBY3JFN2xCYVZWR0JtYzVGb2JnZEpvNCJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMzYzOTExOCwibmJmIjoxNzIzNjM5MTE4LCJleHAiOjE3MjM2NDQ1NzIsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84WEFBQUF6M09QWm9UTW9xaFVPejdGSk9RYnY2MndCcC9DK0hKRmJwZmNlbXlTbG05RE50SGc4c1NCdzQ5cXczaTlVWlV3OFlMWlFCTzRrSjdrZlhZZzgzZnRLL1JaZDdwdWhwd0xhNTR6dE80VFFvaz0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiI2M2Q1OTBiOS0yMzQwLTRlYzgtOTc5OC0yMTU1MzQzYjJkMDAiLCJwdWlkIjoiMTAwMzIwMDMwMzU0MjBGNCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctQWtBQUFBQUFBQUF3QUFBQUFBQUFBQzdBRHMuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUXQ5NGo4U0QxUUlydmxobUxzZElDOXFMSzYxTFZ5UE5tNkR1WEo4RW9IdyIsInRpZCI6IjNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOCIsInVuaXF1ZV9uYW1lIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxpYW0ubGVtZUBNbmdFbnZNQ0FQMDQwNjg1Lm9ubWljcm9zb2Z0LmNvbSIsInV0aSI6IkF4ZG1lWF90cTAtc2xaZkNQaUVIQVEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCIsIjliODk1ZDkyLTJjZDMtNDRjNy05ZDAyLWE2YWMyZDVlYTVjMyIsImE5ZWE4OTk2LTEyMmYtNGM3NC05NTIwLThlZGNkMTkyODI2YyIsImZlOTMwYmU3LTVlNjItNDdkYi05MWFmLTk4YzNhNDlhMzhiMSIsImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfaWRyZWwiOiIxIDgiLCJ4bXNfcGwiOiJlbi1VUyJ9.kM9Mf-WrlQqAVuRbxfeMpMaRjJasSKwLjWb1PW21OzezhGPoKs2oi80WJdPYUZ9_5k_AFdC3ECzACWZiumKqvhbDfACuuRVg-eblaFn0O6jzwB8x3jYXfwRfTJ3ANd4yovDfTwk19boLQ5DhD2B3OpAPGuHHYWdm34rB18EYgbP-YZSVj1WqadXF7PyIMNPT_dSX6lBktkv_ksQ84krcnmKA2TwtHpgf-XDbVcmdeAQ-k3BnBZP80eF25NRpEZtcMPJygXleyb47-rhbL9UqBdfZllmS2fM8OdqpsDch9qkYUtEtu8vZLICLLU6g6y2zflWHq5BTX017BtCr9O5jmA',
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

 
#Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
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

# ### Get workspace

# CELL ********************

url_get= f"https://api.fabric.microsoft.com/v1/workspaces/d21dfb2b-a460-4822-8724-39cef0dc18f2"
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
    "displayName": "Lemedemo_WS2",
    ##"capacityId" : "36C0FFCC-E4A5-401C-8719-3C465D225C4B"
}


#jsonPayload = {
#    "displayName": "Lemedemo_WS2",
#}



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

    workspace_id = workspace_id




   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Assign workspace capacity

# CELL ********************

from urllib.parse import urlparse

# Define the URL
url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/assignToCapacity"


jsonPayload = {
    "capacityId" : "36C0FFCC-E4A5-401C-8719-3C465D225C4B"
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

workspace_id =  "c7660f4d-ec4f-4ac8-85f3-78a2b67e0621"

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

# ### Add permission

# CELL ********************



principal_id = "908e6e3d-deed-4ef0-9a04-cc30bad96e7d"###my user ID
##principal_id =  "1382bbf1-6d66-48b3-8374-b6b7173211e7"
workspace_id = "63077c1c-b5ad-461f-a5ab-386fc11f11f7"

##workspace_id = 

#Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
url_patch= f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments/{principal_id}"
jsonPayload = {
  "role": "contributor"
}


response_status = requests.patch(url_patch,json=jsonPayload,  headers=headers)
response_data = response_status.json()
status = response_data.get('status')

print (response_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create Pipeline

# CELL ********************


import requests

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"


url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines"

payload = {
    "displayName": "Create_Pipl_API1",
    "description": "A data pipeline created  by Rest API"
}

# Make the POST request
response = requests.post(url_pipd,json=payload,  headers=headers)
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

# ### Delete Pipeline

# CELL ********************


import requests

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"
pipelineid= "4362c9e0-c7de-4700-b47f-c59b5fd3992"


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

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"
pipelineid= "4362c9e0-c7de-4700-b47f-c59b5fd3992"


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

# ### Get Data Pipeline

# CELL ********************


import requests

workspace_id = "d21dfb2b-a460-4822-8724-39cef0dc18f2"
#pipelineid= "4362c9e0-c7de-4700-b47f-c59b5fd3992"
target_display_name = "Create_Pipl_API" ### name of the pipeline 

url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items" ##{pipelineid}"

response = requests.get(url_pipd,  headers=headers)

print (response)

if response.status_code in(200,201,202):
    print("Get request successful.")
    responsejson = response.json().get("value", [])
    data_pipelines = [item for item in responsejson if item['type'] == 'DataPipeline' and item['displayName'] == target_display_name ]

    print(data_pipelines)

##https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api#list-items


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

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"


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

# CELL ********************

import requests

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"


url_lh = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"


# Make the POST request
response = requests.get(url_lh,json=payload,  headers=headers)
# Check for HTTP errors
response.raise_for_status()

print (response)

if response.status_code in (201,202):
    print("Get request successful.")
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

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"
LHID = "7f4702be-455c-4da8-8927-c656b94217e6"


url_lh = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{LHID}"


# Make the POST request
response = requests.delete(url_lh,  headers=headers)
# Check for HTTP errors
response.raise_for_status()

print (response)

if response.status_code in (200,201,202):
    print("delete request successful.")
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

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"


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

workspace_id = "dca1b780-eb7d-4403-be54-b5cdff034e60"
NBID = "92e350a4-10f9-4e7e-8134-b839010f7e1b"

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

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "1382bbf1-6d66-48b3-8374-b6b7173211e7"
client_secret = "Use8Q~przbDyulOCfaIPG6U2tOPO.bjGvtrq_a3M" 

resource = 'https://api.fabric.microsoft.com'
##endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'


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

""" All the different methods which will be used for Fabric using Azure management APIs"""

import time

import requests


base_url: str = "https://management.azure.com"
provider_name: str = "Microsoft.Fabric"
api_version: str = "2022-07-01-preview"


class CapacityManagementApi:
    """Class Doc: Contains all the method which can be used for Fabric Azure Management
    Args:
        manager (TelemetryManager): lseg telemetry manager for logging
        token (str): subscription_id where the capacity will be created in Azure
                     rgroup_name as resource_group where the capacity will be created in Azure.
                     Access Token should cover scope for Azure Management:
                     https://management.azure.com,
        Instantiation:
        CapacityManagementApi(manager, subscription_id, resource_group, access token)
    """

    def __init__(
        self,
        subscription_id: str,
        rgroup_name: str,
        access_token: str,
    ) -> None:
        self.subscription_id = subscription_id
        self.access_token = access_token
        self.provider_name = provider_name
        self.rgroup_name = rgroup_name
        self.api_version = api_version
        self.base_url = base_url
        self.crud_url = (
            f"{self.base_url}/subscriptions/{self.subscription_id}/resourceGroups/{self.rgroup_name}"
            f"/providers/{self.provider_name}/capacities"
        )
        self.headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}


    def capacity_exists(self, capacity_name: str) -> bool:
        """Method def -
        If capacity exist with the name returns True or False
        Args:
        capacity (str): capacity name to check
        Returns:
        True or False. It exists(True), if it fails or not exist(False).
        """
        try:
            url = (
                f"{self.base_url}/subscriptions/{self.subscription_id}"
                f"/providers/{self.provider_name}/capacities?api-version={self.api_version}"
            )
            response = requests.get(url, headers=self.headers, timeout=120)
            capacities = response.json().get("value", [])
            for capacity in capacities:
                while capacity["name"] == capacity_name:
                    print(f"200, {capacity_name} capacity exists")
                    return True
            print(f"0, {capacity_name} capacity does not exists")
            return False
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred in create the capacity: {req_err}")
            return False

    def check_provisioning_state(self, capacity_name: str) -> str:
        """Method def -
            check_provisioning_state capacity
        Args:
            capacity (str): capacity name to be checked
        Returns:
        Code response ,Succeeded ,Provisioning, Failed or Not found(capacity does not exist.)
        Example: (200, '<capacityname> provisioningState Succeeded')
                    (0, '<capacityname> provisioningState Failed')
                    (200, '<capacityname> provisioningState Provisioning')
                    (0,  '<capacityname> capacity does not exist')
        Note:
        it does retry 2x in a interval of 10 seconds
        due to the fact of giving time to a capacity that could be in the process of provision
        """
        if self.capacity_exists(capacity_name):
            max_retries = 2
            retry_count = 0
            while retry_count < max_retries:
                try:
                    url = f"{self.crud_url}/{capacity_name}?api-version={self.api_version}"

                    response = requests.get(url, headers=self.headers, timeout=120)
                    status = response.raise_for_status()
                    json_response = response.json()
                    properties = json_response.get("properties", {})
                    provision = {
                        "provisioningState": properties.get("provisioningState"),
                        "state": properties.get("state"),
                    }
                    if provision["provisioningState"] == "Succeeded":
                        print(f"200,{capacity_name} - {provision}")
                        return "Succeeded"
                    if provision["provisioningState"] == "Provisioning" and retry_count == 2:
                        print(f"200,{capacity_name} - {provision}")
                        return "Provisioning"
                    else:
                        print(f"0,{capacity_name} - {provision} - {status}")
                        retry_count += 1
                        if retry_count < max_retries:
                            print(
                                f"Failed. Retrying after 10 seconds... (Attempt {retry_count} of {max_retries})"
                            )
                            time.sleep(10)
                        else:
                            print("Maximum retries reached. Operation failed.")
                            return "Failed"
                except requests.exceptions.RequestException as e:
                    print(f"Request error -capacity status: {e}")
                    return "Failed"
                except ValueError as e:
                    print(f"JSON decoding error- capacity status: {e}")
                    return "Failed"
                except KeyError as e:
                    print(f"Key error - capacity status: {e}")
                    return "Failed"
        else:
            return "Not found"

    def create_capacity(
        self,
        capacity_name: str,
        sku_name: str,
        sku_tier: str,
        tag_key: str,
        tag_value: str,
        location: str,
        admin_email: str,
    ) -> bool:
        """Method def -
            Create the capacity if it does not exist
        Args:
            capacity_name (str): capacity name
            sku_name (str): for example F2 size.
            sku_tier(str): Fabric
            tag_key(str): Test Key for example
            tag_value(str):Test Value for example
            location(str) : region as for example Uk South , East US etc.
            admin_email(str): email or id of the user.
        Returns:
        Code response Success (true)
                      Fail (false)
        Logs: (201, '<capacityname> capacity created')
                    (409, '<capacityname> capacity already exist'
                    errocode,'<capacityname> capacity creation fail - Error Details' )
        """

        if self.capacity_exists(capacity_name):
            print(f"409, {capacity_name} capacity already exists")
            return False
        try:
            url = f"{self.crud_url}/{capacity_name}?api-version={self.api_version}"
            payload = {
                "sku": {"name": sku_name, "tier": sku_tier},
                "tags": {tag_key: tag_value},
                "location": location,
                "properties": {"administration": {"members": [admin_email]}},
            }
            print(url)
            response = requests.put(url, headers=self.headers, json=payload, timeout=120)
            response.raise_for_status()
            if response.status_code in (201, 200) and self.capacity_exists(capacity_name):
                print(f"201, {capacity_name} capacity created")
                return True
        except requests.exceptions.HTTPError as http_err:
            print(f"Request error in capacity creation: {http_err}")
            return False
        except requests.exceptions.RequestException as req_err:
            print(f"Request error in capacity creation: {req_err}")
            return False
        print(f"0, Unknown error - {capacity_name}")
        return False

    def delete_capacity(self, capacity_name: str) -> bool:
        """Method def -
             Delete an existent capacity
         Args:
             capacity (str): capacity name to be checked
        Returns:
        Code response Success (true)
                       Fail (false)
         Logs:  (202, '<capacityname> capacity deleted')
                 (0, '<capacityname> capacity does not exist'
                 errocode,'<capacityname> capacity failed in been deleted - Error Details' )
        """
        try:
            url = f"{self.crud_url}/{capacity_name}?api-version={self.api_version}"

            if self.capacity_exists(capacity_name):
                response = requests.delete(url, headers=self.headers, timeout=120)
                if response.status_code == 202:
                    print(f"202, {capacity_name} capacity deleted")
                    return True
            else:
                print(f"0, {capacity_name} capacity does not exist")
                return False
        except requests.exceptions.HTTPError as http_err:
            print(f"Request error in deleting the capacity: {http_err}")
            return False
        except requests.exceptions.RequestException as req_err:
            print(f"Request error in deleting the capacity: {req_err}")
            return False
        print(f"0, Unknown error - {capacity_name}")
        return False

    def pause_capacity(self, capacity_name: str) -> bool:
        """Method def -
             Pause an existent capacity
         Args:
             capacity (str): capacity name to be checked
        Returns:
         Code response Success (true)
                        Fail (false)
         Logs: (202, '<capacityname> capacity paused')
                     (400, '<capacityname> capacity is already in pause or in the process to be suspended')
                     (0,   '<capacityname> capacity does not exist'
                     errocode,'<capacityname> capacity failed in been pause - Error Details' )
        """
        try:
            url = f"{self.crud_url}/{capacity_name}" f"/suspend?api-version={self.api_version}"

            if self.capacity_exists(capacity_name):
                response = response = requests.post(url, headers=self.headers, timeout=120)
                if response.status_code == 202:
                    print(f"202, {capacity_name} capacity paused")
                    return True
                if response.status_code == 400:
                    print(f"400, {capacity_name} capacity is already in pause or in the process")
                    return True
            else:
                print(f"0, {capacity_name} capacity does not exist")
                return False
        except requests.exceptions.HTTPError as http_err:
            print(f"Request error in pause capacity: {http_err}")
            return False
        except requests.exceptions.RequestException as req_err:
            print(f"Request error in pause capacity: {req_err}")
            return False
        print(f"0, Unknown error - {capacity_name}")
        return False

    def resume_capacity(self, capacity_name: str) -> bool:
        """Method def -
            Resume an existent capacity
        Args:
            capacity (str): capacity name to be checked
        Returns:
        Code response ,Success ,fail or capacity does not exist, message.
        Example: (202, '<capacityname> capacity resumed')
                    (400, '<capacityname> capacity already has been started or in the process-pause/resume')
                    (0,   '<capacityname> capacity does not exist'
                    errocode,'<capacityname> capacity failed in been resumed - Error Details' )
        """
        try:
            url = f"{self.crud_url}/{capacity_name}" f"/resume?api-version={self.api_version}"

            if self.capacity_exists(capacity_name):
                response = response = requests.post(url, headers=self.headers, timeout=120)
                if response.status_code == 202:
                    print(f"202, {capacity_name} capacity resumed")
                    return True
                if response.status_code == 400:
                    print(f"400, {capacity_name} capacity already has been started")
                    return True
            else:
                print(f"0, {capacity_name} capacity does not exist")
                return False
        except requests.exceptions.HTTPError as http_err:
            print(f"Request error in resume capacity: {http_err}")
            return False
        except requests.exceptions.RequestException as req_err:
            print(f"Request error in resume capacity: {req_err}")
            return False
        print(f"0, Unknown error - {capacity_name}")
        return False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

get_auth_token_SP("aa", "aa","https://management.azure.com")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url= "https://management.azure.com/subscriptions/78479cb4-e81a-4926-8c84-fa9c7784069b/resourceGroups/SQL-HA-RG-Li/providers/Microsoft.Fabric/capacities/leme?api-version=2022-07-01-preview"

payload = {
    "sku": {
        "name": "F2",
        "tier": "Fabric"
    },
    "tags": {
        "testKey": "testValue"
    },
    "location": "UK South",
    "properties": {
        "administration": {
            "members": [
                "lmpserviceacdatabase-c@lmsp0.onmicrosoft.com", "liliam.leme-c@lmsp0.onmicrosoft.com"
            ]
        }
    }
}
response = requests.put(url, headers=headers, json=payload, timeout=120)
response.raise_for_status()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


access_token= "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyIsImtpZCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuYXp1cmUuY29tIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMTgyODc3NiwibmJmIjoxNzIxODI4Nzc2LCJleHAiOjE3MjE4MzI2NzYsImFpbyI6IkUyZGdZRGhaWDdXYWcvZDgwSG5QSXh6Yi9mNkhBZ0E9IiwiYXBwaWQiOiJjNTJlYWZiNy03Mzg3LTQ2NmUtYWQ0MS05ODE2NGE3MzI1NmEiLCJhcHBpZGFjciI6IjEiLCJncm91cHMiOlsiMmU1MWI4MWYtNjFlMC00YTQ2LTg1MmUtNGQ3NTQwMjVlYjJkIiwiMjBiNmQ2N2UtNjhhYi00MTg1LWE1ZWYtZmNiMWY2YjNkNDI1IiwiNTBlM2YyNGEtZjE0NS00NTFhLTg5MzItYjU5N2E2NTdhNTY4IiwiNjAyNTU5OTMtYzgzYy00NmM2LWFiOTgtYTMyMWZkZmNjYjcwIl0sImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOC8iLCJpZHR5cCI6ImFwcCIsIm9pZCI6IjQzZTViNjQzLWNkMTAtNDVkZS1hNWJlLWE5ODYxOWJiNTEzZCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctRVpJZjNrQXV0ZFB1a1Bhd2ZqMk1CTzdBQUEuIiwic3ViIjoiNDNlNWI2NDMtY2QxMC00NWRlLWE1YmUtYTk4NjE5YmI1MTNkIiwidGlkIjoiM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4IiwidXRpIjoib0hZeEVNeTI0a0tkT1BCWUU2VzZBQSIsInZlciI6IjEuMCIsInhtc19pZHJlbCI6IjcgMjYiLCJ4bXNfdGNkdCI6MTY3Njg5MTE1NH0.d2d0Q1A44jvjsWWBRW9Qk1KW4IPXr_sFDXAxzeGjLwkAAq4GDzAqrHXBoZMZpOlxCl6_MpDK6RhyHo83m5_1TAAIqjdbVMSKcfJq7Hz2WxiCCmRh6SgNMVv-6ctsvHFZ8VLa9SxWVvR-UttznVTLuMnoMMJcfZl2xwYi9OmIEOLJlJVIRgNsRybGVQyexr_8CBnNRbACRLxr22f1QhSymlK4jPxqBKeip9Ce9jbDmqUmYWay7lrfMMSpiRk2I1MH9JLxAI2Wqsg5rO2FLljFUdMU32sokGDeJZUQ4fL3tHW6Z4mEadgDDdlR9GRpEw0OCBAKCOR-Jp-Kxq6Jt9JJvw"
#headers = {
#    'Authorization': f'Bearer {access_token}',
#    'Content-Type': 'application/json',
#    'Accept': 'application/json'
#}
#client = CapacityManagementApitest_nolog(subscription_id, access_token,resource_group_name)


subscription_id = "78479cb4-e81a-4926-8c84-fa9c7784069b"
rgroup_name = "SQL-HA-RG-Li"
provider_name = "Microsoft.Fabric"
capacity_name = "leme_oxygenteste"
api_version = "2022-07-01-preview"

sku_name = "F2"
sku_tier = "Fabric"
tag_key = "testKey"
tag_value = "testValue"
location = "UK South"
admin_email =  "iliam.leme@MngEnvMCAP040685.onmicrosoft.com"
tenant_id = '3c863c9b-2221-4236-88c3-37fe9e1d06f8'



#logger.error('Notebook is parameter client_id is missing')

client = CapacityManagementApi( subscription_id, rgroup_name,access_token )

#clientlog = CapacityManagementApi_onlylogs(manager, subscription_id, access_token,rgroup_name)

# Call the create_capacity method
response = client.create_capacity( capacity_name,  sku_name, sku_tier, tag_key, tag_value, location, admin_email)
response ## management api scope

#response = client.check_provisioning_state(capacity_name)
#response


#response = client.capacity_exists(capacity_name)
#response

#response = client.pause_capacity(capacity_name)
#response


#response = client.resume_capacity(capacity_name)
#response


#response = client.delete_capacity(capacity_name)
#response

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

""" All the different methods which will be used for Fabric using Azure management APIs"""

import requests

base_url: str = "https://api.fabric.microsoft.com/v1"
provider_name: str = "Microsoft.Fabric"
api_version: str = "2022-07-01-preview"


class CapacityFabricApitest:
    """Class Doc: Contains all the method which can be used for Fabric Azure Management
    Args:
                       https://api.fabric.microsoft.com/v1
    """

    def __init__(
        self,

        subscription_id: str,
        access_token: str,
        rgroup_name: str,
    ) -> None:
        self.subscription_id = subscription_id
        self.access_token = access_token
        self.provider_name = provider_name
        self.rgroup_name = rgroup_name
        self.api_version = api_version
        self.base_url = base_url
    


    def capacity_exists_in_fabric(self, capacity_name: str) -> str:
        """Method def - return the new capacity id given a name
        rgs:
        capacity_name (str): capacity name
        Returns:
            int: "0" the capacity does not exist
                Id of the capacity
                Additional details will be found in the logs.
        Note: The capacity id is not an ARM response so
            the Fabric API must be call( instead of the management one) only in this case
        """
        url_fabric = "https://api.fabric.microsoft.com/v1"
        url_capacity = f"{url_fabric}/capacities"
        response = requests.get(url_capacity, headers=self.headers, timeout=120)
        capacities = response.json().get("value", [])

        for capacity in capacities:
            if capacity["displayName"] == capacity_name:
                print(f"Successfully found the capacity - StatusCode: {response.status_code}\n"
                      f"capacity: {capacity_name}")                      
                capacity_id: str = capacity["id"]
                return capacity_id
        print(f"capacity  was not found" f"- StatusCode: {response.status_code}\n" f"capacity: {capacity_name}"     )
        return "0"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Getting the values of the workspace through the variables


# CELL ********************

import os
import sys
import platform
import psutil

def get_environment_details():
    # Gather environment details
    environment_details = {
        "Python Version": sys.version,
        "Platform": platform.platform(),

        "Current Working Directory": os.getcwd(),
        "Environment Variables": dict(os.environ)
    }

    # Filter environment variables for 'SPARK_YARN_STAGING_DIR'
    #spark_yarn_staging_dir_filtered = {key: value for key, value in os.environ.items() if 'SPARK_YARN_STAGING_DIR' in key}

    # Display the result
    #print(spark_yarn_staging_dir_filtered)
    # Print environment details
    for key, value in environment_details.items():
       print(f"{key}: {value}")
    # Filter environment variables for 'SPARK_YARN_STAGING_DIR'


get_environment_details()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import sys
import platform
import psutil


environment_details = { "Environment Variables": dict(os.environ)}
    

# Filter environment variables for 'SPARK_YARN_STAGING_DIR'
spark_yarn_staging_dir_filtered = {key: value for key, value in os.environ.items() if 'SPARK_YARN_STAGING_DIR' in key}

# Display the result
print(spark_yarn_staging_dir_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
