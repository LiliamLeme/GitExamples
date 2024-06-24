# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9d7d7d21-6095-4ad0-a610-a5e68c6c061c",
# META       "default_lakehouse_name": "Rest_LH",
# META       "default_lakehouse_workspace_id": "937c925f-aa0c-42f0-851e-084f6a233d3c",
# META       "known_lakehouses": [
# META         {
# META           "id": "9d7d7d21-6095-4ad0-a610-a5e68c6c061c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

pip install requests

# CELL ********************

import requests

# Define your Azure subscription, resource group, and other parameters
subscription_id = "78479cb4-e81a-4926-8c84-fa9c7784069b"
resource_group_name = "SQL-HA-RG-Li"
dedicated_capacity_name = "lemefabric"
api_version = "2022-07-01-preview"

# Define your Azure AD parameters
grant_type = "client_credentials"
client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
client_secret = "rCH8Q~b2WipnAoFGt2k68RX7qNHWEWGB14UbucEP"
auth_url = "https://login.microsoftonline.com/MngEnvMCAP040685.onmicrosoft.com/oauth2/token"

##Suspend
resource_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Fabric/capacities/{dedicated_capacity_name}/suspend?api-version={api_version}"
##resume
resource_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Fabric/capacities/{dedicated_capacity_name}/resume?api-version={api_version}"

# Request an access token
token_response = requests.post(
    auth_url,
    data={
        "grant_type": grant_type,
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://management.azure.com/",
    },
)
token = token_response.json()
access_token = token["access_token"]

# Include the Authorization header
headers = {
    "Authorization": f"Bearer {access_token}",
}

# Make the request
response = requests.post(resource_url, headers=headers)

# Output the response
print(response.json())

