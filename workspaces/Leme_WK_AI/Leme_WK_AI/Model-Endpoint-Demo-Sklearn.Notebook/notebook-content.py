# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f35b0518-b19b-470b-92c5-685d460aa13a",
# META       "default_lakehouse_name": "Leme_LH_AI",
# META       "default_lakehouse_workspace_id": "a854d162-6bf9-4073-917e-14a11ce272aa"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Model Endpoint Demo: Sklearn Logistic Regression Model
# 
# This notebook is a demo test for the Model Endpoint feature due to Private Preview soon as part of the Microsoft Fabric Data Science Experience. Using this feature, Fabric users can expose a public URL as an API endpoint to query/score their MLFlow Models using a JSON payload for their test dataset.
# 
# ##### This notebook does the following:
# 1. Uses the model/version that you provide in cell#1
# 1. Activates Model Endpoint feature for the model created in above step.
# 1. Provision & Bootstrap Model Endpoint on this model (**This step may take 5-10min or maybe longer**)
#     ```
#     Please note that this explicit step is added to gracefully handle the cold startup time (5-10min) when endpoint is accessed the very first time.
#     ```
# 1. Run 5 identical score calls to this model endpoint
# 
# ##### Feel free to:
# 1. Create your own custom model (Change in Cell 5: Create the test model)
# 1. Use your own custom dataset (Change in Cell 11: Get Test Input Data and Query Model Endpoint)
# 1. Skip deleting/cleaning for later use (Set `cleanup_endpoint_resources=False` in Cell#2: Customize Setup to SKIP deleting the endpoint)
# 
# **Please make sure that if you skip deleting resources right now, you DO delete the resources once you are done testing as unused resource(s) may cause capacity issues for testing**

# CELL ********************

###################
# Customize Setup
###################
model_name = 'mleme-bugbash-sklearn-lr_leme' # [STRING] Set it to a Model Name of your choice
model_version = 1                      # [INTEGER] On new model creation, it creates version 1. If this is an older model, use specific version here.
cleanup_endpoint_resources = True      # Cleans up Model Endpoint Resources, Model and the experiment.

# CELL ********************

#####################################################
# Import dependencies for the notebook
#####################################################
import time
import json
import pandas as pd
from synapse.ml.mlflow import get_mlflow_env_config
import requests
from synapse.ml.fabric import FabricCredential

# CELL ********************

#####################################################
# Helper Utilities
#####################################################
experiment = None         # INTERNAL-PURPOSES ONLY
MAX_RETRY_COUNT = 10      # INTERNAL-PURPOSES ONLY

def call_public_api(method=requests.get, sub_path='', data=None, show_elapsed_time=False, retry_on_503=False):
    token = FabricCredential().get_token()
    headers = dict()
    headers['Authorization'] = f'Bearer {token.token}'

    retry_count = 1
    while retry_count != MAX_RETRY_COUNT:
        start = time.time()
        resp = method(f'{public_api_endpoint_base_url}{sub_path}', headers=headers, data=data)
        elapsed_time = time.time() - start
        if resp.status_code != 200 and resp.status_code != 202:
            if resp.status_code == 503 and retry_on_503:
                print(f"Retry {retry_count}/{MAX_RETRY_COUNT}: Retrying in 15sec as Runner is reporting unavailable.")
                time.sleep(15)
                retry_count += 1
            else:
                raise Exception(f"Request failed with status code {resp.status_code}, Reason: {resp.text}")
        else:
            if show_elapsed_time:
                print(f'The request took {elapsed_time} seconds')
            return resp.text
        
    raise Exception(f"MaxRetry attempts reached: Request failed with status code {resp.status_code}, Reason: {resp.text}")

# CELL ********************

####################################################################
# Create the test model 
#
# NOTE: YOU CAN REPLACE THIS WITH YOUR CUSTOM CODE TO CREATE MODEL
####################################################################

import mlflow
import numpy as np 
from sklearn.linear_model import LogisticRegression 
from sklearn.datasets import load_diabetes
from mlflow.models.signature import infer_signature 

experiment = mlflow.set_experiment(model_name)
with mlflow.start_run() as run:
    lr = LogisticRegression()
    data = load_diabetes(as_frame=True)
    lr.fit(data.data, data.target) 
    signature = infer_signature(data.data, data.target) 

    mlflow.sklearn.log_model(
        lr,
        model_name,
        signature=signature,
        registered_model_name=model_name
    )

# CELL ********************

#####################################################
# Load the model to get debugging logs for artifact id
#####################################################
import logging
import mlflow
from datetime import datetime

ts = datetime.now().strftime('%Y%m%d%H%M%S')
log_filename=f'tmp-{ts}.log'
file_handler = logging.FileHandler(log_filename)

logging.getLogger('synapse.ml.mlflow').setLevel(logging.DEBUG)
logging.getLogger('synapse.ml.mlflow').addHandler(file_handler)
mlflow.pyfunc.load_model(f'models:/{model_name}/{model_version}')
logging.getLogger('synapse.ml.mlflow').setLevel(logging.INFO)

# CELL ********************

#####################################################
# Extract artifact ID from logs (previous cell)
#####################################################
failure = True
artifact_id = None

with open(log_filename, 'r') as fp:
    lines = fp.readlines()
    for line in lines:
        line = line.replace(" ", "")  # strip spaces
        if line.find('onelake_path') != -1:
            parts = line.split(":")
            assert len(parts) == 4
            artifact_path = parts[3]
            parts = artifact_path.split("/")
            assert len(parts) == 3
            artifact_id = parts[0]
            failure = False

if failure or artifact_id == None or artifact_id == '':
    print('Failed to get artifact id')
    sys.exit(-1)

# CELL ********************

#####################################################
# Build Public API URL
#####################################################
env_configs = get_mlflow_env_config()
if env_configs.pbienv == 'edog':
    public_api_host = 'https://powerbiapi.analysis-df.windows.net'
elif env_configs.pbienv == 'daily':
    public_api_host = 'https://dailyapi.fabric.microsoft.com'
elif env_configs.pbienv == 'dxt':
    public_api_host = 'https://dxtapi.fabric.microsoft.com'
elif env_configs.pbienv == 'msit':
    public_api_host = 'https://msitapi.fabric.microsoft.com'
else:
    public_api_host = 'https://api.fabric.microsoft.com'

workspace_id = env_configs.workspace_id
capacity_id = env_configs.capacity_id
artifact_id = artifact_id
public_api_endpoint_base_url = f'{public_api_host}/v1/workspaces/{workspace_id}/MLModels/{artifact_id}/endpoint'

print("############################# DEBUG INFORMATION ################################")
print('Model Name    : ', model_name)
print('Model Version : ', model_version)
print('Workspace ID  : ', workspace_id)
print('Capacity ID   : ', capacity_id)
print('Artifact ID   : ', artifact_id)
print('Public API URL: ', public_api_endpoint_base_url)
print("#################################################################################")

# CELL ********************

#####################################################
# Activate Endpoint for this Model
#####################################################
activate_endpoint_json_payload = '{"enabled": true, "defaultVersion": "' + str(model_version) + '"}'
resp = call_public_api(sub_path='/configuration', method=requests.patch, data=activate_endpoint_json_payload)
print(f'Endpoint Activated - Response : {resp}')

# CELL ********************

#####################################################
# Provision Endpoint Resources
#####################################################
resp = call_public_api(sub_path=f'/versions/{model_version}', method=requests.patch, data=activate_endpoint_json_payload)
print(f'Endpoint Provisioning Started: {resp}')
provisioning_start_ts = datetime.now()

#####################################################
# Wait for model endpoint to be ready (Running state)
#####################################################
sleep_interval = 20  # in seconds
sleep_time_counter = 0
sleep_time_max = 60*60 # 1 hour

status = None
while True:
    resp = call_public_api(method=requests.get)
    try:
        content_dict = json.loads(resp)
        if 'versions' not in content_dict:
            raise Exception('Invalid JSON BODY response. Missing versions')

        versions = content_dict['versions']
        if not isinstance(versions, list):
            raise Exception('Invalid JSON BODY response. versions is not a list')

        found_version = None
        for ver in versions:
            if 'version' not in ver or 'status' not in ver:
                raise Exception('Invalid JSON BODY response. version/status not found inside a version field.')
            if ver['version'] == str(model_version):
                found_version = ver
        if not found_version:
            raise Exception(f'Invalid JSON Body: Version {model_version} not found')
        status = found_version['status']
        if status.lower() == 'running':
            break
        else:
            if sleep_time_counter > sleep_time_max:
                raise Exception(f'Reached threshold for provisioning endpoint for version {model_version}. Please report it to: model-endpoints-core@microsoft.com.') 
            print(f'Waiting for version {model_version} to get to RUNNING state. Retrying in {sleep_interval} seconds. Please wait...')
            time.sleep(sleep_interval)
            sleep_time_counter += sleep_interval
    except Exception as exc:
        print(f'Exception in handling JSON response: {exc}') 
        raise exc
    
print('#####################################################')
print(f'PROVISIONING SUCCESS: The endpoint is now in {status} state !')
provisioning_end_ts = datetime.now()
print('#####################################################')
print(f'Provisioning started at {provisioning_start_ts.strftime("%Y.%m.%d %H:%M:%S")}')
print(f'Provisioning succeeded at {provisioning_end_ts.strftime("%Y.%m.%d %H:%M:%S")}')
print(f'Time elapsed in provisioning: {provisioning_end_ts - provisioning_start_ts}')
print('#####################################################')

# CELL ********************

#################################################################################################
# Get Test Input Data and Query Model Endpoint (REPLACE DATA WITH YOUR CUSTOM DATA)
#################################################################################################
from sklearn.datasets import load_diabetes

api_sub_path = f'/versions/{model_version}'
data = load_diabetes(as_frame=True)
pd_values = pd.DataFrame(data=data['data'], columns=data['feature_names']).head().to_json(orient="values")
input_df_json_string = '{"format":"dataframe", "orientation":"values", "inputs": ' + f'{json.loads(pd_values)}' + '}'

for i in range(5):
    print(f'Prediction Response: {call_public_api(sub_path=api_sub_path, method=requests.post, data=input_df_json_string, show_elapsed_time=True, retry_on_503=True)}')

# CELL ********************

#####################################################
# Cleanup Endpoint Resources
#####################################################
if cleanup_endpoint_resources:
    print('Cleaning up:\n\t(1) Model Endpoint Resources')
    print(f'deleting model endpoint resources for {model_name}')
    print(f'Response: {call_public_api(method=requests.delete)}')
else:
    print(f'Not doing cleanup as "cleanup_endpoint_resources" set to {cleanup_endpoint_resources}')
    print('==========================================================================')
    print('============================ A T T E N T I O N ===========================')
    print('==========================================================================')
    print('\033[1mAS YOU HAVE DECIDED TO SKIP RESOURCES CLEANUP RIGHT NOW, YOU DO DELETE\033[0m')
    print('\033[1mTHE RESOURCES ONCE YOU ARE DONE TESTING AS UNUSED RESOURCE(S) MAY CAUSE CAPACITY ISSUES FOR TESTING\033[0m')
    print('==========================================================================')
