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

Display name:new_api_fabric
Application (client) ID:a49b035c-f602-4b26-be68-784fb7c374c9
Object ID:f44c41e5-0778-4d84-a15a-163bd6ca1f50
Directory (tenant) ID:3c863c9b-2221-4236-88c3-37fe9e1d06f8
Use8Q~przbDyulOCfaIPG6U2tOPO.bjGvtrq_a3M

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Variables

# MARKDOWN ********************

# #### With minimum permissions

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "a82e6252-7aa7-45d3-83ea-4474b9c3da31"
client_secret = "I.f8Q~jEJu~cqVWzP2QPneA9ofOAU6qxRyUmEbVf" 




resource = 'https://api.fabric.microsoft.com'
##endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get credential token(SP)

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
    'Authorization': f'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IktRMnRBY3JFN2xCYVZWR0JtYzVGb2JnZEpvNCIsImtpZCI6IktRMnRBY3JFN2xCYVZWR0JtYzVGb2JnZEpvNCJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMzgwMTgwOCwibmJmIjoxNzIzODAxODA4LCJleHAiOjE3MjM4MDcyNzAsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84WEFBQUFaS3daaWIySFljN1R5aS9KUnZaVE5JOTdKZVdqeTVqK2dIM3IrSkp6SVZOcVlYSWxManBXZ29QeFB6MFJiMURobDhpZUtwaWJ5a1N4cmtzOHptL05sV2hQWkhqVUlsRlEzY1l1ZmdrbEJRTT0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiI2M2Q1OTBiOS0yMzQwLTRlYzgtOTc5OC0yMTU1MzQzYjJkMDAiLCJwdWlkIjoiMTAwMzIwMDMwMzU0MjBGNCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctQWtBQUFBQUFBQUF3QUFBQUFBQUFBQzdBRHMuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUXQ5NGo4U0QxUUlydmxobUxzZElDOXFMSzYxTFZ5UE5tNkR1WEo4RW9IdyIsInRpZCI6IjNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOCIsInVuaXF1ZV9uYW1lIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxpYW0ubGVtZUBNbmdFbnZNQ0FQMDQwNjg1Lm9ubWljcm9zb2Z0LmNvbSIsInV0aSI6ImdHVkpzcWdacEVxaC1GZVl3Z2dSQUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCIsIjliODk1ZDkyLTJjZDMtNDRjNy05ZDAyLWE2YWMyZDVlYTVjMyIsImE5ZWE4OTk2LTEyMmYtNGM3NC05NTIwLThlZGNkMTkyODI2YyIsImZlOTMwYmU3LTVlNjItNDdkYi05MWFmLTk4YzNhNDlhMzhiMSIsImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfaWRyZWwiOiIyMCAxIiwieG1zX3BsIjoiZW4tVVMifQ.keerGnhAevV5CV90eG-K4Jmxu4t97RFOiViPlPnJTZCYUjZ5eXcMrh7RITpwgLwjvYqNxf1M3htKd4QkpqSMOypA6Ni3adJolM9XTVTGKkdPA4hLErwB6c-RzjIHnqpAyMEX1PWmJS3v8mmDOTCIwCrZKZG6sfTyWDhDCO_Sm8e6P-OgWBR7cCs1SNMzP93xo3IklC3DpnM6hnSQYpUtaEvxibPocWfoFCJ1BrVauxZ_QXVVM3YWVAD--ZpOB6cgD6UyQ3wZpox8Zq8iSCXKX60SaxWjdBt6_aqmAogj40kfyVHE_Q0X8VkooJCoSGDNABJxvWG14Hq6EXSEHjg0bA',
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

# ### Workspace Permission

# CELL ********************



principal_id = "a82e6252-7aa7-45d3-83ea-4474b9c3da31"###my user ID
##principal_id =  "1382bbf1-6d66-48b3-8374-b6b7173211e7"
workspace_id = "d21dfb2b-a460-4822-8724-39cef0dc18f2"

##workspace_id = 

#Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
url_patch= f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments/{principal_id}"
jsonPayload = {
  "role": "viewer"
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

# ### List workspace

# CELL ********************

 
#Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
url_get= f"https://api.fabric.microsoft.com/v1/workspaces"
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

import requests
from urllib.parse import urlparse

# Define the URL
url = "https://api.fabric.microsoft.com/v1/workspaces"

jsonPayload = {
    "displayName": "Lemedemo_WS2"
    ##"capacityId" : "02A066CC-5DBF-4B85-A25F-76C50F991CFC" ###it does not work with trial, but works with the capacity created.
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

workspace_id =  "416283d6-721a-4b01-86b4-8b9ebff7ee37"

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

workspace_id = "4210d5f9-6c19-49fe-88ba-366e3d72c296"


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

# CELL ********************

headers = {
    'Authorization': f'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyIsImtpZCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMTY0NjQzNSwibmJmIjoxNzIxNjQ2NDM1LCJleHAiOjE3MjE2NTEwODksImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84WEFBQUFDTUxGcWtpQ0toT3VBQ2ZuQXh3UXRzM3BDODFhc3NXcDc1MUN3S2ZOQ1ZvUFZkV01OZEQybXBudDNKYWF0L2lNbkRKQjBRQXB4ZWpLUmlhZmExU2NyaWU1Z0VQOWdIWVNPODkxcE90YTVSVT0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiI2M2Q1OTBiOS0yMzQwLTRlYzgtOTc5OC0yMTU1MzQzYjJkMDAiLCJwdWlkIjoiMTAwMzIwMDMwMzU0MjBGNCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctQWtBQUFBQUFBQUF3QUFBQUFBQUFBQzdBRHMuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUXQ5NGo4U0QxUUlydmxobUxzZElDOXFMSzYxTFZ5UE5tNkR1WEo4RW9IdyIsInRpZCI6IjNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOCIsInVuaXF1ZV9uYW1lIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxpYW0ubGVtZUBNbmdFbnZNQ0FQMDQwNjg1Lm9ubWljcm9zb2Z0LmNvbSIsInV0aSI6ImhzUkRLay1XMDBXd1BWRnpxeTU1QUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCIsIjliODk1ZDkyLTJjZDMtNDRjNy05ZDAyLWE2YWMyZDVlYTVjMyIsImE5ZWE4OTk2LTEyMmYtNGM3NC05NTIwLThlZGNkMTkyODI2YyIsImZlOTMwYmU3LTVlNjItNDdkYi05MWFmLTk4YzNhNDlhMzhiMSIsImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfaWRyZWwiOiIxIDIwIiwieG1zX3BsIjoiZW4tVVMifQ.qX22ejtXcwVPH5pPVcfKDCqYtv0GWZiMxlv1mc_XyYhHJKlkNBkJTOsomTYn9dxVpnb9RPrhpbI1UhH58c3NaRvNc8y16Gge0Npn26N7zfPgKj-Kuntet8F2Mxtu8odOF6JRzmqQ7A-W6f5dxk-GhAymFOqpKG9MZt_uTKrJ8IgG86fYpyF3_Mh6Dw0R2eUX0y5-YcZLB132Ej0iSDT1pPK3lFwxCaUmT3E9vSA4y0kwMib93PQzXW9g1p142N4W6ru4EYtdc08JuERMzHXdy83rPQLi4NTuSxjeQjzMphb7GCLW18yTo1doCcdJW4TBW7UDr2spHZrd97vFvyT4cQ',
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

# ### Executing Data PIpelines

# CELL ********************


import requests

workspace_id = "d21dfb2b-a460-4822-8724-39cef0dc18f2"
pipelineid= "7b101d18-721a-4a1e-84b6-087a008c3bbd"


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

# CELL ********************

###Fabric data pipeline public REST API (Preview) - Microsoft Fabric | Microsoft Learn
###https://learn.microsoft.com/en-us/fabric/data-factory/pipeline-rest-api#run-on-demand-item-job
import requests

workspace_id = "d21dfb2b-a460-4822-8724-39cef0dc18f2"
pipelineid= "7b101d18-721a-4a1e-84b6-087a008c3bbd"


url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipelineid}/jobs/instances?jobType=Pipeline"
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

workspace_id = "8c872e53-3ab3-47d1-ad64-d3c8cbfe9467"
pipelineid= "4362c9e0-c7de-4700-b47f-c59b5fd3992"


url_pipd = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines/"

response = requests.get(url_pipd,  headers=headers)
print (response)


if response.status_code in(200,201,202):
    print("Get request successful.")
else:
    print(f"Error: {response.status_code} - {response.text}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get Data Pipeline list

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

# MARKDOWN ********************

# ### Create workspace

# MARKDOWN ********************

# #### Management APIs.
# ##### Create the token and execute the request

# CELL ********************

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "1382bbf1-6d66-48b3-8374-b6b7173211e7"
client_secret = "rXu8Q~jKRyxwEclBP8PKIeucH5x3iBN5ILfebaoi" 

resource = 'https://management.azure.com'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Replace these values with your actual authentication and resource details
##APP_Fabric
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
client_secret = "UcA8Q~aPHPnoJ~xI1Qu8ra-bZB_BH82XP-yJ4dyI" 


resource = 'https://management.azure.com'

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

#token_response

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

import json

# system parameters
api_version     = "2022-07-01-preview"
url_base        = "https://management.azure.com/subscriptions/"
url_rg          = "/resourceGroups/"
resource_group_name         = "SQL-HA-RG-Li"
url_provider    = "/providers/Microsoft.Fabric/capacities/"
capacity_name = "fabrictest"
subscription_id = "78479cb4-e81a-4926-8c84-fa9c7784069b"
capacity_admins = "liliam.leme@MngEnvMCAP040685.onmicrosoft.com", "63d590b9-2340-4ec8-9798-2155343b2d00"
capacity_tags = "test "
capacity_region = "West US 3"
capacity_sku = "F2"



#build url
url = f'{url_base}{subscription_id}{url_rg}{resource_group_name}{url_provider}{capacity_name}?api-version={api_version}'

#build request body
#payload =  {
#        'sku':  {
#                'name': 'F2',
#                'tier':'Fabric'
#                },
#        
#        'location':capacity_region,
#        'properties':   {
#                        'administration':   {
#                                            'members':  capacity_admins
#                                            }
#                        },
#        'tags':capacity_tags
#        }
#print(url, payload)


payload = {
    "sku": {
        "name": "F2",
        "tier": "Fabric"
    },
    "tags": {
        "testKey": "testValue"
    },
    "location": "West US 3",
    "properties": {
        "administration": {
            "members": [
                "liliam.leme@MngEnvMCAP040685.onmicrosoft.com"
            ]
        }
    }
}




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from urllib.parse import urlparse
import requests
url= "https://management.azure.com/subscriptions/78479cb4-e81a-4926-8c84-fa9c7784069b/resourceGroups/SQL-HA-RG-Li/providers/Microsoft.Fabric/capacities/leme?api-version=2022-07-01-preview"
payload 

# Make the POST request
response = requests.put(url, headers=headers, json=payload)
# Check for HTTP errors
response.raise_for_status()
print (response)
##https://learn.microsoft.com/en-us/rest/api/power-bi-embedded/capacities/create?view=rest-power-bi-embedded-2021-01-01&tabs=HTTP

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Resume

# CELL ********************

import requests

# Variables (you need to set these values)
auth_url = "https://login.microsoftonline.com/MngEnvMCAP040685.onmicrosoft.com/oauth2/token"
resource = "https://management.azure.com/"
capacity_name = "lemecopilot"

# Request an access token
payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'resource': resource
}

response = requests.post(auth_url, data=payload)
response.raise_for_status()  # Raise an exception for HTTP errors

token = response.json()['access_token']

# Construct the API request URL
url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Fabric/capacities/{capacity_name}/resume?api-version={api_version}"

# Include the Authorization header
headers = {
    'Authorization': f"Bearer {token}"
}

# Make the request
response = requests.post(url, headers=headers)
response.raise_for_status()  # Raise an exception for HTTP errors
url


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Pause

# CELL ********************

import requests

# Variables (you need to set these values)
auth_url = "https://login.microsoftonline.com/MngEnvMCAP040685.onmicrosoft.com/oauth2/token"
resource = "https://management.azure.com/"
capacity_name = "lemecopilot"

# Request an access token
payload = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'resource': resource
}

response = requests.post(auth_url, data=payload)
response.raise_for_status()  # Raise an exception for HTTP errors

token = response.json()['access_token']

# Construct the API request URL
url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.Fabric/capacities/{capacity_name}/suspend?api-version={api_version}"

# Include the Authorization header
headers = {
    'Authorization': f"Bearer {token}"
}

# Make the request
response = requests.post(url, headers=headers)
response.raise_for_status()  # Raise an exception for HTTP errors
url


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
