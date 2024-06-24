# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c79d2bcd-61e4-4e24-af61-e61d1e5a4510",
# META       "default_lakehouse_name": "lh_demo",
# META       "default_lakehouse_workspace_id": "3d631752-8cfd-48dd-a1e4-192d37311254"
# META     }
# META   }
# META }

# CELL ********************

#####################################################
import time
import json
import pandas as pd
from synapse.ml.mlflow import get_mlflow_env_config
import requests
from synapse.ml.fabric import FabricCredential

token = FabricCredential().get_token()
headers = dict()
headers['Authorization'] = f'Bearer {token.token}'


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

# CELL ********************

import requests
from synapse.ml.fabric import FabricCredential

token = FabricCredential().get_token()
headers = dict()
headers['Authorization'] = f'Bearer {token.token}'


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
