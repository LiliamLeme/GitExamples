# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fc9fb29f-b1ad-448e-b32c-e0be6dd5f5c4",
# META       "default_lakehouse_name": "datbricksOnelake",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "fc9fb29f-b1ad-448e-b32c-e0be6dd5f5c4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

##custom library.
from math_custom import add_2
                
print (add_2.add_2(1,2))

##https://learn.microsoft.com/en-us/fabric/data-engineering/library-management#manage-custom-libraries-in-workspace-setting
##https://packaging.python.org/en/latest/tutorials/packaging-projects/
## Quick Start — The Hitchhiker's Guide to Packaging 1.0 documentation (the-hitchhikers-guide-to-packaging.readthedocs.io)
