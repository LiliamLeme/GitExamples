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

""" All the different methods which will be used for Fabric using Azure management APIs"""

import time

import requests


base_url: str = "https://management.azure.com"
provider_name: str = "Microsoft.Fabric"
api_version: str = "2022-07-01-preview"


class CapacityApi:

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
        try:
            url = (
                f"{self.base_url}/subscriptions/{self.subscription_id}"
                f"/providers/{self.provider_name}/capacities?api-version={self.api_version}"
            )
            print (url)
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

        if self.capacity_exists(capacity_name):
            max_retries = 2
            retry_count = 0
            while retry_count < max_retries:
                try:
                    url = f"{self.crud_url}/{capacity_name}?api-version={self.api_version}"
                    print (url)
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
    )  -> bool:
        if self.capacity_exists(capacity_name):
            print(f"409, {capacity_name} capacity already exists")
            return False
        try:
            url = f"{self.crud_url}/{capacity_name}?api-version={self.api_version}"
            payload = {
                "sku": {"name": sku_name, "tier": sku_tier},
                "tags": {tag_key: tag_value},
                "location": location,
                "properties": {"administration": {"members": admin_email}},
            }
            print(url)
            print(payload)
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
            print(url)
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
            print(url)
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
            print(url)
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

# MARKDOWN ********************

# ## Create Capacity

# CELL ********************


subscription_id = "78479cb4-e81a-4926-8c84-fa9c7784069b"
rgroup_name = "SQL-HA-RG-Li"
provider_name = "Microsoft.Fabric"
capacity_name = "leme_oxygenteste1"
api_version = "2022-07-01-preview"



url= f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{rgroup_name}/providers/{provider_name}/capacities/{capacity_name}?api-version=2022-07-01-preview"
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
        "liliam.leme@MngEnvMCAP040685.onmicrosoft.com"
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


#access_token= "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyIsImtpZCI6Ik1HTHFqOThWTkxvWGFGZnBKQ0JwZ0I0SmFLcyJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuYXp1cmUuY29tIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcyMTgyODc3NiwibmJmIjoxNzIxODI4Nzc2LCJleHAiOjE3MjE4MzI2NzYsImFpbyI6IkUyZGdZRGhaWDdXYWcvZDgwSG5QSXh6Yi9mNkhBZ0E9IiwiYXBwaWQiOiJjNTJlYWZiNy03Mzg3LTQ2NmUtYWQ0MS05ODE2NGE3MzI1NmEiLCJhcHBpZGFjciI6IjEiLCJncm91cHMiOlsiMmU1MWI4MWYtNjFlMC00YTQ2LTg1MmUtNGQ3NTQwMjVlYjJkIiwiMjBiNmQ2N2UtNjhhYi00MTg1LWE1ZWYtZmNiMWY2YjNkNDI1IiwiNTBlM2YyNGEtZjE0NS00NTFhLTg5MzItYjU5N2E2NTdhNTY4IiwiNjAyNTU5OTMtYzgzYy00NmM2LWFiOTgtYTMyMWZkZmNjYjcwIl0sImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOC8iLCJpZHR5cCI6ImFwcCIsIm9pZCI6IjQzZTViNjQzLWNkMTAtNDVkZS1hNWJlLWE5ODYxOWJiNTEzZCIsInJoIjoiMC5BWHdBbXp5R1BDRWlOa0tJd3pmLW5oMEctRVpJZjNrQXV0ZFB1a1Bhd2ZqMk1CTzdBQUEuIiwic3ViIjoiNDNlNWI2NDMtY2QxMC00NWRlLWE1YmUtYTk4NjE5YmI1MTNkIiwidGlkIjoiM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4IiwidXRpIjoib0hZeEVNeTI0a0tkT1BCWUU2VzZBQSIsInZlciI6IjEuMCIsInhtc19pZHJlbCI6IjcgMjYiLCJ4bXNfdGNkdCI6MTY3Njg5MTE1NH0.d2d0Q1A44jvjsWWBRW9Qk1KW4IPXr_sFDXAxzeGjLwkAAq4GDzAqrHXBoZMZpOlxCl6_MpDK6RhyHo83m5_1TAAIqjdbVMSKcfJq7Hz2WxiCCmRh6SgNMVv-6ctsvHFZ8VLa9SxWVvR-UttznVTLuMnoMMJcfZl2xwYi9OmIEOLJlJVIRgNsRybGVQyexr_8CBnNRbACRLxr22f1QhSymlK4jPxqBKeip9Ce9jbDmqUmYWay7lrfMMSpiRk2I1MH9JLxAI2Wqsg5rO2FLljFUdMU32sokGDeJZUQ4fL3tHW6Z4mEadgDDdlR9GRpEw0OCBAKCOR-Jp-Kxq6Jt9JJvw"
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
admin_email =  ["liliam.leme@MngEnvMCAP040685.onmicrosoft.com"]
tenant_id = '3c863c9b-2221-4236-88c3-37fe9e1d06f8'



#logger.error('Notebook is parameter client_id is missing')

client = CapacityApi( subscription_id, rgroup_name,access_token )

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

https://management.azure.com/subscriptions/78479cb4-e81a-4926-8c84-fa9c7784069b/providers/Microsoft.Fabric/capacities?api-version=2022-07-01-preview
0, leme_oxygenteste capacity does not exists
https://management.azure.com/subscriptions/78479cb4-e81a-4926-8c84-fa9c7784069b/resourceGroups/SQL-HA-RG-Li/providers/Microsoft.Fabric/capacities/leme_oxygenteste?api-version=2022-07-01-preview
{'sku': {'name': 'F2', 'tier': 'Fabric'}, 'tags': {'testKey': 'testValue'}, 'location': 'UK South', 'properties': {'administration': {'members': ['liliam.leme@MngEnvMCAP040685.onmicrosoft.com']}}}
Request error in capacity creation: 400 Client Error: Bad Request for url: https://management.azure.com/subscriptions/78479cb4-e81a-4926-8c84-fa9c7784069b/resourceGroups/SQL-HA-RG-Li/providers/Microsoft.Fabric/capacities/leme_oxygenteste?api-version=2022-07-01-preview
False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
