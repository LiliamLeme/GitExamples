# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ddd3c697-3976-4bcc-87e6-94b8babfcbc6",
# META       "default_lakehouse_name": "Airlift_LH",
# META       "default_lakehouse_workspace_id": "44e31f04-34e7-4ac8-909a-2663460eea4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "ddd3c697-3976-4bcc-87e6-94b8babfcbc6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import re

def generate_string(input_text):
    pattern = r'dev\/[a-z]+\/[a-z0-9-]+'

    # Check if the input matches the pattern
    if re.fullmatch(pattern, input_text):
        return input_text
    else:
        raise ValueError("Input does not match the expected pattern.")

# Example usage
try:
    generated_string = generate_string('dev/stagging/1_patch-liliam')
    print("Generated String:", generated_string)
except ValueError as e:
    print(e)


generate_string('liliam')
