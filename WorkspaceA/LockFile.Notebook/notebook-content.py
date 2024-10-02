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

import os

def lockfile(action):
    if action == 'C':  # create
        some_content = 'dummy_lock_workload'

        with open('/lakehouse/default/Files/dummy_lock_workload.txt', 'w') as f:
            f.write(some_content)
    elif action == 'D':  # drop
        file_path = '/lakehouse/default/Files/dummy_lock_workload.txt'
        try:
            os.remove(file_path)
        except FileNotFoundError:
            print(f"The file '{file_path}' does not exist.")
    else:
        print('No valid action provided.')

# Example usage:
lockfile('C')  # Create lock file
lockfile('D')  # Drop (delete) lock file
lockfile('X')  # No valid action provided

