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

# ##### This one does not work - Resize

# CELL ********************

import requests
import time


base_url: str = "https://management.azure.com"
provider_name: str = "Microsoft.Fabric"
api_version: str = "2022-07-01-preview"

class ResizetApi:

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
                f"/providers/{self.provider_name}/capacities/"
            )
            self.headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}

        def resize_capacity(self, capacity_name: str, sku_name: str, sku_tier: str, tag_key: str, tag_value: str, admin_emails: list):
                """Method def -
                    Resize an existent capacity
        
                Args:
                    capacity (str): capacity name to check
        
                Returns:
                Code response ,Success ,fail or capacity does not exist, message.
                Example: (202, 'fabrictest capacity resized from value to value')
                            (0,   'fabrictest capacity does not exist'
                            errocode,'fabrictest capacity failed in been pause - Error Details' )
                """
                try:    

                    url = f"{self.base_url}/subscriptions/{self.subscription_id}/resourceGroups/{self.rgroup_name}/providers/{self.provider_name}/capacities/{capacity_name}?api-version=api-version={self.api_version}"
                    print(url)
                    payload = {
                        "sku": {
                            "name": sku_name,
                            "tier": sku_tier
                        },
                        "tags": {
                            tag_key: tag_value
                        },
                        "properties": {
                            "administration": {
                                "members": [admin_emails]
                            }
                        }
                    }

                    response = requests.patch(url, headers=self.headers, json=payload)
                    response.raise_for_status()
                    if response.status_code in (202,200):
                        return response.status_code, f'{capacity_name} capacity resized to {sku_name} '
                except Exception as err:
                    return  response.status_code, f'{capacity_name} capacity failed in been pause - {err}' 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#from azure_api import AzureAPIClient
# Define variables for creating a capacity
resource_group_name         = "SQL-HA-RG-Li" ##==Check the RG
capacity_name = "fabrictest123"
subscription_id = "78479cb4-e81a-4926-8c84-fa9c7784069b"
admin_email = "liliam.leme@MngEnvMCAP040685.onmicrosoft.com"##Deployment user
sku_name = "F2"
sku_tier = "Fabric"
tag_key = "testKey"
tag_value = "testValue"
location = "West US 3" ## UK south

client = ResizetApi(subscription_id, resource_group_name,access_token)

response = client.resize_capacity( capacity_name,  sku_name, sku_tier, tag_value, location, admin_email)
response



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
