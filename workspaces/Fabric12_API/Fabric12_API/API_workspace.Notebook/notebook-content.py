# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

import requests
import time
from urllib.parse import urlparse



# Include the Authorization header
####f12-> Console -> copy(powerBIAccessToken)
headers = {
    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImtXYmthYTZxczh3c1RuQndpaU5ZT2hIYm5BdyIsImtpZCI6ImtXYmthYTZxczh3c1RuQndpaU5ZT2hIYm5BdyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTcwODY5NjY1MywibmJmIjoxNzA4Njk2NjUzLCJleHAiOjE3MDg3MDE5NTEsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBYlFBUy84V0FBQUFUNUplVndnczg5Ty9SUXR4MXZwbWdMRkpVRHh0cnZpdHRMYlhlRjZ2RS8rb0gvYmkzSjlVdEt4TGxtKzhDV3E2Rmh2VUQwWTBlZnY5UjhReDZ1WWFJVUs0NVpObHBEV3NhSXIwSGVYZFZIZFR6YXBtYk9XNVN2OFhWbFhhVEJqTVZiUlRtWVVRSTZDdmJXTGtad0xlL2dLK3d4RWFWdnBZditYM2ZHWEZmMXptVng4SkpId3ZjT0tMTGxlV0xuN0F5UVlOSngrNVFqMUtGeHBRU3hHVk1TTTdYVldqQlhFM2RMekgvNUJscmVVPSIsImFtciI6WyJyc2EiLCJtZmEiXSwiYXBwaWQiOiI4NzFjMDEwZi01ZTYxLTRmYjEtODNhYy05ODYxMGE3ZTkxMTAiLCJhcHBpZGFjciI6IjAiLCJjb250cm9scyI6WyJhcHBfcmVzIl0sImNvbnRyb2xzX2F1ZHMiOlsiMDAwMDAwMDktMDAwMC0wMDAwLWMwMDAtMDAwMDAwMDAwMDAwIiwiMDAwMDAwMDMtMDAwMC0wZmYxLWNlMDAtMDAwMDAwMDAwMDAwIl0sImRldmljZWlkIjoiMjY3NzhiZDAtMzUyZi00Y2U2LThiMWEtNDEzODNjZmFiZWFmIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiJlNmU3MTAwNS1kMGZiLTRmZmUtODcxMC1lMzUyYzM2MmZkMTYiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMTcyMTI1NDc2My00NjI2OTU4MDYtMTUzODg4MjI4MS0zODc2MDQ4IiwicHVpZCI6IjEwMDM3RkZFODdFMUNERkQiLCJyaCI6IjAuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUndrQUFBQUFBQUFBd0FBQUFBQUFBQUFhQUZJLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInNpZ25pbl9zdGF0ZSI6WyJkdmNfbW5nZCIsImR2Y19jbXAiLCJrbXNpIl0sInN1YiI6IjhYQW5YLVBaTUlMcjM3bUdyNmRlVi1MSVA0bTE4d3ZEUVRYVlVjQ3lMNVEiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6ImxpbGVtQG1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxlbUBtaWNyb3NvZnQuY29tIiwidXRpIjoiQ2N3Ynd3dTRvMDJvOTVaNlRJZ1dBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19jYyI6WyJDUDEiXX0.L76bsgfvfJNnMfPMf1ZJ-3rbzTv8R4JWhMER_lXZrj12UWSD-pqZYMcEHwq9sn3sPZhz-bvdNZtkYvuKlHJ2W3gylRmZbQNhFveRngteEEwBY4LA2WqJVGlV5Ds9tJvlNHMdWSV5rWmvYuKC0PgVpcZXF-HPj2UFeCgkdD3C7SBzirygmYUyqcfdJgFU0QRWn5O7rgQ8EMRAu-VMB_EJ_94A9IPp-4t6cClD0TLUhQ0KaDBNhEDOSPr4W21fgPCNlrS0JE6wU8HplUqDx0DgRgrwkvadO1zHDBQfDuE3CMJhk8YsPobxJsL9Yy9wao9YXVEfs_YGQbI9U_wTdR-E2w",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create Workspace

# CELL ********************

# Define the URL
url = "https://api.fabric.microsoft.com/v1/workspaces/"


jsonPayload = {
    "displayName": "lemedemo_test12"
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

# MARKDOWN ********************

# ### Check Workspace creation

# CELL ********************

 
#Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
url_get= f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
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

# ### Add Permissions

# CELL ********************

principal_id = "e6e71005-d0fb-4ffe-8710-e352c362fd16"###my user ID
workspace_id = "5985f182-59e8-4de5-a721-a063bd14c8e9"

#Location: https://api.fabric.microsoft.com/v1/workspaces/cfafbeb1-8037-4d0c-896e-a46fb22287ff
url_patch= f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/roleAssignments/{principal_id}"
jsonPayload = {
  "role": "admin"
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

# ### Delete workspace

# CELL ********************

import requests

workspace_id = "5985f182-59e8-4de5-a721-a063bd14c8e9"

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

# ### List all workspaces

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
