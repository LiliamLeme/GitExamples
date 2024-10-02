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

# Include the Authorization header

headers = {
    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjVCM25SeHRRN2ppOGVORGMzRnkwNUtmOTdaRSIsImtpZCI6IjVCM25SeHRRN2ppOGVORGMzRnkwNUtmOTdaRSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcwNTQxMjU2MCwibmJmIjoxNzA1NDEyNTYwLCJleHAiOjE3MDU0MTY1MTMsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84VkFBQUFvNmNBVlBxaXBxelJycFVYVHVFUmFIaldIT09tV0tncUxRKzFRaDFROTV4bnJUaHBpUzZYRVZDV3dKdzVmQVd1IiwiYW1yIjpbInB3ZCJdLCJhcHBpZCI6Ijg3MWMwMTBmLTVlNjEtNGZiMS04M2FjLTk4NjEwYTdlOTExMCIsImFwcGlkYWNyIjoiMCIsImZhbWlseV9uYW1lIjoiTGVtZSIsImdpdmVuX25hbWUiOiJMaWxpYW0iLCJpcGFkZHIiOiI4MS4xMDYuMjAwLjc1IiwibmFtZSI6IkxpbGlhbSBMZW1lIiwib2lkIjoiNjNkNTkwYjktMjM0MC00ZWM4LTk3OTgtMjE1NTM0M2IyZDAwIiwicHVpZCI6IjEwMDMyMDAzMDM1NDIwRjQiLCJyaCI6IjAuQVh3QW16eUdQQ0VpTmtLSXd6Zi1uaDBHLUFrQUFBQUFBQUFBd0FBQUFBQUFBQUM3QURzLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInN1YiI6IlF0OTRqOFNEMVFJcnZsaG1Mc2RJQzlxTEs2MUxWeVBObTZEdVhKOEVvSHciLCJ0aWQiOiIzYzg2M2M5Yi0yMjIxLTQyMzYtODhjMy0zN2ZlOWUxZDA2ZjgiLCJ1bmlxdWVfbmFtZSI6ImxpbGlhbS5sZW1lQE1uZ0Vudk1DQVAwNDA2ODUub25taWNyb3NvZnQuY29tIiwidXBuIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1dGkiOiJyR0FxdmFFYWgwbTRvTk5hbzZpMUFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJhOWVhODk5Ni0xMjJmLTRjNzQtOTUyMC04ZWRjZDE5MjgyNmMiLCJmZTkzMGJlNy01ZTYyLTQ3ZGItOTFhZi05OGMzYTQ5YTM4YjEiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3BsIjoiZW4tVVMifQ.1_y2kxu7EEC6Z-B23XBfE0Q6KVo5v_OC4a_G0tWAr4T4r3EBY0CaZ1lbcofhifCMWAygx9cBkjLEpq4R_tXUYkwsikQvTZ1z6M5owjLU9VUPQcoKUCudDahftF7fZKgKJF-N43GeRcE0qqMumUHUQDwwDJ9olx7snCJrQv92k7wUt_ALp0T-o6L52IEQrOFk2yduT7HeRbr0Hap95xD0UokU4zQhUpDdZbmH7yMIlkzntdIysdvT-GAuqXvDqdMYOVPJxE48xht4kwW7J6xaSLHtW7lE8iwtBbaUA-XP4uJqBVp01_7_5_LrOxGkiiwmdlAn69cHCuJT3RTSh7AQbQ",
}

# MARKDOWN ********************

# ### Execution

# CELL ********************

##THIS IS A SAMPLE not for Production##

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

   
    location_header = response.headers.get("Location")
    parsed_url = urlparse(location_header)
    path_parts = parsed_url.path.split('/')
    job_instance_id = path_parts[-1] 

    print(location_header)
    print(job_instance_id)

    url_get= f"https://api.fabric.microsoft.com/v1/workspaces/{wokspace}/items/{pipeline}/jobs/instances/{job_instance_id}"
    response_status = requests.get(url_get, headers=headers)
    
    # Check the response. If 202, the pipeline job was queue
    if response_status.status_code == 202:
        #url_get= f"https://api.fabric.microsoft.com/v1/{workspaces/wokspace}/items/{pipeline}/jobs/instances/{job_instance_id}"
        #print (url_get)

        ###STATUS
        response_status = requests.get(url_get, headers=headers)
        #print (response_status)
        
    while True:
        # Make the GET request
        response_status = requests.get(url_get, headers=headers)
        #print(response_status)

        if response_status.status_code == 200:
            try:
                
                response_data = response_status.json()
                status = response_data.get('status')

                #print (response_data)

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

  
