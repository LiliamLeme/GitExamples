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

import os

some_content = 'dummy_lock_workload'
file_path = '/lakehouse/default/Files/dummy_lock_workload.txt'


# Check if the file exists
if os.path.exists(file_path):
    print(f"The file '{file_path}' exists.")
else:
    print(f"The file '{file_path}' does not exist.")

os.path.exists(file_path)
timeout_seconds = 600  # timeout, 600 seconds, 10 min
start_time = time.time()


print (os.path.exists(file_path))

while os.path.exists(file_path)=='True':
    os.path.exists(file_path)
    time.sleep(15)



# CELL ********************

import os
import time

target_directory = "/lakehouse/default/Files/"
os.chdir(target_directory)
print("Current Working Directory:", os.getcwd())

file_path = "dummy_lock_workload.txt"
full_file_path = os.path.join(target_directory, file_path)

file_exists = os.path.exists(full_file_path)
print("File Exists:", file_exists)

timeout_seconds = 600  # timeout, 600 seconds, 10 min
start_time = time.time()

print("File path:", full_file_path)

if file_exists:
    print("File exists!")
else:
    print("File does not exist.")


# CELL ********************

import os

some_content = 'dummy_lock_workload'
file_path = '/lakehouse/default/Files/dummy_lock_workload.txt'


# Check if the file exists
if os.path.exists(file_path):
    print(f"The file '{file_path}' exists.")
else:
    print(f"The file '{file_path}' does not exist.")

