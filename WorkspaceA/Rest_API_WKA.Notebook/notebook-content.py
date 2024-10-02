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
# META       "default_lakehouse_workspace_id": "d21dfb2b-a460-4822-8724-39cef0dc18f2",
# META       "known_lakehouses": [
# META         {
# META           "id": "c557e0a3-13eb-43df-8750-bfbf032e663c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Parameters for execution

# CELL ********************

import requests
import time
from urllib.parse import urlparse

# Define your Azure AD parameters
grant_type = "client_credentials"
client_id = "908e6e3d-deed-4ef0-9a04-cc30bad96e7d"
client_secret = "kHz8Q~UlROmkvI-ZqTdCwg6yssGVhPf~uCnlpcvj"  


auth_url = "https://login.microsoftonline.com/MngEnvMCAP040685.onmicrosoft.com/oauth2/token"

# Request an access token
token_response = requests.post(
    auth_url,
    data={
        "grant_type": grant_type,
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://api.fabric.microsoft.com/"
    },
)
token = token_response.json()
access_token = token["access_token"]

# Include the Authorization header
####f12-> Console -> copy(powerBIAccessToken)
headers = {
    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyIsImtpZCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMjI1OTkwNywibmJmIjoxNzIyMjU5OTA3LCJleHAiOjE3MjIyNjQwNTMsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84WEFBQUFJZ0JkVnFIODFtZG41c0VYT0tPRFVoQU9kKzlaVnJ4WDRjNlZmbzF2YmVpZXhqaHlCc1gveTVwajlMUjRBc1J2RTNkM2JiMGhwZG9BWUN1cGxvU2NkbytqRmsxOXc1Ung5alRHZWhNSjFPaz0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiI2M2Q1OTBiOS0yMzQwLTRlYzgtOTc5OC0yMTU1MzQzYjJkMDAiLCJwdWlkIjoiMTAwMzIwMDMwMzU0MjBGNCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctQWtBQUFBQUFBQUF3QUFBQUFBQUFBQzdBRHMuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUXQ5NGo4U0QxUUlydmxobUxzZElDOXFMSzYxTFZ5UE5tNkR1WEo4RW9IdyIsInRpZCI6IjNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOCIsInVuaXF1ZV9uYW1lIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxpYW0ubGVtZUBNbmdFbnZNQ0FQMDQwNjg1Lm9ubWljcm9zb2Z0LmNvbSIsInV0aSI6ImVORWs3TjBpMVVlVngydDFUR1FxQUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCIsIjliODk1ZDkyLTJjZDMtNDRjNy05ZDAyLWE2YWMyZDVlYTVjMyIsImE5ZWE4OTk2LTEyMmYtNGM3NC05NTIwLThlZGNkMTkyODI2YyIsImZlOTMwYmU3LTVlNjItNDdkYi05MWFmLTk4YzNhNDlhMzhiMSIsImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfaWRyZWwiOiIxIDIwIiwieG1zX3BsIjoiZW4tVVMifQ.w4HhND4RKR3Up9dQFja8akvc1-_mOwvRhZb25rB8OaOu2GUW-WPXx9vxXGqUSaHJpFRNfscMXYipmCmqRuGyaMkZyXdimgNxrmBksm4Of5BhnvYrSkoD0E9BrJI2xHWZ3-I4t8gvBwZX72ZT2H1eDMFaT7OxRpTAEf6LDlffoemjTTApvuHfvWjqwXYp0oaFXjUG01tzSNFnzUyVHvLvw8B3-MluFxr9FwXNowzHuZdPt3KWvqlOcebRqeyJTWrC3O_KC6TXHkXQhC9iowncw8jNrUdP2RqD_j8vPlrKBGi5WVY-u5SBis8oxWpr3c9rnrV4MPlmjlG9FxmLEi4IVg",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

headers = {
    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyIsImtpZCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMjI1OTkwNywibmJmIjoxNzIyMjU5OTA3LCJleHAiOjE3MjIyNjQwNTMsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVlFBcS84WEFBQUFJZ0JkVnFIODFtZG41c0VYT0tPRFVoQU9kKzlaVnJ4WDRjNlZmbzF2YmVpZXhqaHlCc1gveTVwajlMUjRBc1J2RTNkM2JiMGhwZG9BWUN1cGxvU2NkbytqRmsxOXc1Ung5alRHZWhNSjFPaz0iLCJhbXIiOlsicHdkIiwibWZhIl0sImFwcGlkIjoiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiYXBwaWRhY3IiOiIwIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlkdHlwIjoidXNlciIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiI2M2Q1OTBiOS0yMzQwLTRlYzgtOTc5OC0yMTU1MzQzYjJkMDAiLCJwdWlkIjoiMTAwMzIwMDMwMzU0MjBGNCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctQWtBQUFBQUFBQUF3QUFBQUFBQUFBQzdBRHMuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoiUXQ5NGo4U0QxUUlydmxobUxzZElDOXFMSzYxTFZ5UE5tNkR1WEo4RW9IdyIsInRpZCI6IjNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOCIsInVuaXF1ZV9uYW1lIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxpYW0ubGVtZUBNbmdFbnZNQ0FQMDQwNjg1Lm9ubWljcm9zb2Z0LmNvbSIsInV0aSI6ImVORWs3TjBpMVVlVngydDFUR1FxQUEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjYyZTkwMzk0LTY5ZjUtNDIzNy05MTkwLTAxMjE3NzE0NWUxMCIsIjliODk1ZDkyLTJjZDMtNDRjNy05ZDAyLWE2YWMyZDVlYTVjMyIsImE5ZWE4OTk2LTEyMmYtNGM3NC05NTIwLThlZGNkMTkyODI2YyIsImZlOTMwYmU3LTVlNjItNDdkYi05MWFmLTk4YzNhNDlhMzhiMSIsImI3OWZiZjRkLTNlZjktNDY4OS04MTQzLTc2YjE5NGU4NTUwOSJdLCJ4bXNfaWRyZWwiOiIxIDIwIiwieG1zX3BsIjoiZW4tVVMifQ.w4HhND4RKR3Up9dQFja8akvc1-_mOwvRhZb25rB8OaOu2GUW-WPXx9vxXGqUSaHJpFRNfscMXYipmCmqRuGyaMkZyXdimgNxrmBksm4Of5BhnvYrSkoD0E9BrJI2xHWZ3-I4t8gvBwZX72ZT2H1eDMFaT7OxRpTAEf6LDlffoemjTTApvuHfvWjqwXYp0oaFXjUG01tzSNFnzUyVHvLvw8B3-MluFxr9FwXNowzHuZdPt3KWvqlOcebRqeyJTWrC3O_KC6TXHkXQhC9iowncw8jNrUdP2RqD_j8vPlrKBGi5WVY-u5SBis8oxWpr3c9rnrV4MPlmjlG9FxmLEi4IVg",
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Execution

# CELL ********************

import requests
import time
from urllib.parse import urlparse

wokspace = "d21dfb2b-a460-4822-8724-39cef0dc18f2"
pipeline = "8c438f77-f972-4433-9ece-b8f19c76b82b"

url = f"https://api.fabric.microsoft.com/v1/workspaces/{wokspace}/items/{pipeline}/jobs/instances?jobType=Pipeline"

payload = {
    "executionData": {}
}

# Make the POST request
response = requests.post(url,json=payload,  headers=headers)
# Check for HTTP errors
response.raise_for_status()

if response.status_code == 200 or response.status_code == 202:
    print("Request accepted. Processing in progress.")

    #print (location_header)

    location_header = response.headers.get("Location")
    parsed_url = urlparse(location_header)
    path_parts = parsed_url.path.split('/')
    job_instance_id = path_parts[-1] 

    url_get= f"https://api.fabric.microsoft.com/v1/workspaces/{wokspace}/items/{pipeline}/jobs/instances/{job_instance_id}"
    response_status = requests.get(url_get, headers=headers)



    # Check the response. If 202, the pipeline job was queue
    if response.status_code == 202:
        #url_get= f"https://api.fabric.microsoft.com/v1/{workspaces/wokspace}/items/{pipeline}/jobs/instances/{job_instance_id}"
        #print (url_get)

        ###STATUS
        response_status = requests.get(url_get, headers=headers)
        print (response_status)
        
    while True:
        # Make the GET request
        response_status = requests.get(url_get, headers=headers)
        #print(response_status)

        if response_status.status_code == 200:
            try:
                
                response_data = response_status.json()
                status = response_data.get('status')

                print (response_data)

                if status == 'Completed':
                    print("Status:", status)
                    break  # Exit the loop if status is "Succeeded"
                else:
                    print("Execution is still in progress. Checking again...")
                    print("Status:", status)
            except json.JSONDecodeError:
                print("JSON Decode Error. Retrying...")

        else:
            #print("Error:", response_status.status_code)
            print('break - finished')
            #print("Execution is complete. Final status code:", response_status.status_code)
            break 

        
        time.sleep(20)

  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
