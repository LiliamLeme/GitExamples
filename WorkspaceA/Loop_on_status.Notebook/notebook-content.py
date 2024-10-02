# Fabric notebook source


# MARKDOWN ********************

# ### I need to finish, parameterize from the pipeline the runid and loop until the status is not correct.

# CELL ********************

job_instance_id = 

# CELL ********************

import requests
import time
from urllib.parse import urlparse
import json




###docs:https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-public-api#run-a-notebook-on-demand
###https://review.learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/get-item-job-instance?branch=drafts%2Ffeatures%2Fga-release&tabs=HTTP

#GET https://api.fabric.microsoft.com/v1/admin/workspaces/{workspaceId}/items/{itemId}
##f12-> Console -> copy(powerBIAccessToken)

# Define your Azure AD parameters
grant_type = "client_credentials"
client_id = "908e6e3d-deed-4ef0-9a04-cc30bad96e7d"
client_secret = "kHz8Q~UlROmkvI-ZqTdCwg6yssGVhPf~uCnlpcvj"  

#client_id = "c52eafb7-7387-466e-ad41-98164a73256a"####"c52eafb7-7387-466e-ad41-98164a73256a"
#client_secret = "vQG8Q~Kj3Dqv4.UJpoMY2rCeuOULaos5ud.Tkaaw" ## ##"rCH8Q~b2WipnAoFGt2k68RX7qNHWEWGB14UbucEP" ##"vQG8Q~Kj3Dqv4.UJpoMY2rCeuOULaos5ud.Tkaaw"
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
    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlQxU3QtZExUdnlXUmd4Ql82NzZ1OGtyWFMtSSIsImtpZCI6IlQxU3QtZExUdnlXUmd4Ql82NzZ1OGtyWFMtSSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcwMjQ3NTY1MCwibmJmIjoxNzAyNDc1NjUwLCJleHAiOjE3MDI0Nzk3ODksImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84VkFBQUFVUi9PdVg0NzBoRkJtcVRlem0xaFREN01mMHROcEQ3Mzh2cWhUVjFVZ1hsRWRSVTRqUDJzb3NUeVpUeUtTVE5vIiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6Ijg3MWMwMTBmLTVlNjEtNGZiMS04M2FjLTk4NjEwYTdlOTExMCIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiTGVtZSIsImdpdmVuX25hbWUiOiJMaWxpYW0iLCJpcGFkZHIiOiI4MS4xMDYuMjAwLjc1IiwibmFtZSI6IkxpbGlhbSBMZW1lIiwib2lkIjoiNjNkNTkwYjktMjM0MC00ZWM4LTk3OTgtMjE1NTM0M2IyZDAwIiwicHVpZCI6IjEwMDMyMDAzMDM1NDIwRjQiLCJyaCI6IjAuQVh3QW16eUdQQ0VpTmtLSXd6Zi1uaDBHLUFrQUFBQUFBQUFBd0FBQUFBQUFBQUM3QURzLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6IlF0OTRqOFNEMVFJcnZsaG1Mc2RJQzlxTEs2MUxWeVBObTZEdVhKOEVvSHciLCJ0aWQiOiIzYzg2M2M5Yi0yMjIxLTQyMzYtODhjMy0zN2ZlOWUxZDA2ZjgiLCJ1bmlxdWVfbmFtZSI6ImxpbGlhbS5sZW1lQE1uZ0Vudk1DQVAwNDA2ODUub25taWNyb3NvZnQuY29tIiwidXBuIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1dGkiOiJ3YUZmbFpDZTVreXhHREtEblRJdUFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJhOWVhODk5Ni0xMjJmLTRjNzQtOTUyMC04ZWRjZDE5MjgyNmMiLCJmZTkzMGJlNy01ZTYyLTQ3ZGItOTFhZi05OGMzYTQ5YTM4YjEiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3BsIjoiZW4tVVMifQ.TtQGcCaXlnXdM0w4K3CUrKppyQ3uUfhnDHkn3Nj1xw76PNmvCj2oDV9waNYDMbLAkPW1lbsDV5VKn458Zjk_QZ0sFVgAe7qvr0GGe0aLrE73BLE39agrCgGfhex-i-fAlkf1nHH3GvPqsQiRupeeUVnCil9F9f56xqnc4p5Df0KGpdd3atsXW3yUJnj8U4Zr_kcrpdQycOe4zpE4Up96DPXOVrZPq5aLyM7v7rL77joXokvCKw1HUlNdohTXo3KxWBloFAcSx33BUa_VPzwCywiXyjR4sdIMBrQq7XgA2h98wnkuTNpbYES0ucRAM6Jkf04JSjGXTOceJ-LUNe1cqg",
}



 ##checking execution status of the pipeline
url_get= f"https://api.fabric.microsoft.com/v1/workspaces/937c925f-aa0c-42f0-851e-084f6a233d3c/items/51364473-1039-47b2-8bcd-0a02b6c90e73/jobs/instances/{job_instance_id}"
#print (url_get)

while status<>"Suceed"

response_status = requests.get(url_get, headers=headers)
        
# waiting 15 seconds until the status change, on it change it means it complete
if response_status.status_code == 202 or response_status.status_code == 200:
    print("Execution is still in progress. Checking again...")
    response_data = response_status.json()

    # Extract the status
    status = response_data.get('status')

    # Print the result
    print("Status:", status)

