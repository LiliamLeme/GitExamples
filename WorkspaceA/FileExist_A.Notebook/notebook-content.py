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

# CELL ********************

import os.path
import time
import sys


some_content = 'dummy_lock_workload'
file_path = '/lakehouse/default/Files/dummy_lock_workload.txt'
os.path.exists(file_path)

timeout_seconds = 600  # timeout, 600 seconds, 10 min
start_time = time.time()


while os.path.exists(file_path):
    os.path.exists(file_path)
    time.sleep(15)

if os.path.exists(file_path):
    print('File exists!')
else:
    print('File does not exist or the event timed out')
    ##sys.exit("Timeout occurred. Exiting the notebook.")

