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

# MARKDOWN ********************

# ### 1) The reason for the import logging
# 
# https://getdocs.org/Python/docs/3.10/library/concurrent.futures
# 
# 
# https://docs.python.org/3/howto/logging-cookbook.html#formatting-times-using-utc-gmt-via-configuration
# 
# 
# https://blog.fabric.microsoft.com/en-us/blog/fabric-changing-game-logging-your-workload-using-notebooks/

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import os
from concurrent.futures import ThreadPoolExecutor
import traceback
import logging
import time

#date for the name format
datestr = time.strftime("_%Y%m%d_T_%H%M")
#date for the log
datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")
#print (datestr)
#https://docs.python.org/3/howto/logging-cookbook.html#formatting-times-using-utc-gmt-via-configuration




mssparkutils.fs.mkdirs('abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Log_Parallel/') ##create folder if does not exist
log_path ='/lakehouse/default/Files/Log_Parallel/'
file_name = 'Parallel_notebooks_info.log_' + datestr

logging.basicConfig(filename=log_path + file_name+'.txt',  force = True
                    , filemode='w'
                    #, format='%(name)s - %(levelname)s - %(message)s'
                    #,datefmt='%m-%d %H:%M'
                    ,level=logging.INFO)##or DEBUG
timeout = 3600
#Define the folder path for the error log

notebooks = [
    {"path": "/Notebook_interactive", "params": {"parameterString":"Production.Product"}},
    {"path": "/Notebook_interactive", "params": {"parameterString":"AAA"}},
    {"path": "/Notebook_interactive", "params": {"parameterString":"Production.WorkOrder"}}
    ##{"path": "notebook3", "params": {"param3": "value3"}},
    ]

def func_notebook_Error_handle(notebook):
    try:
        mssparkutils.notebook.run(notebook["path"], timeout, notebook["params"])
        message = f"- {datestr_log}(UTC) - yeahh! ruunning...Notebook executed '{notebook['path']} , {notebook['params']} '\n"
        logging.info(message)
    except Exception as e:
        error_message = f"- {datestr_log}(UTC) - whoops! Exception occurred in notebook '{notebook['path']} , {notebook['params']}'\n\n Follow the error:\n\n {e}\n"
        #print(error_message)
        #traceback_info= 'Follow the traceback: \n' + traceback.format_exc() + "\n"
        #error_message += traceback_info
        logging.critical(error_message)

 

# Create a ThreadPoolExecutor
with ThreadPoolExecutor() as executor:
    # Submit notebook executions to the executor
    notebook_tasks = [executor.submit(func_notebook_Error_handle, notebook) for notebook in notebooks]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Logging - Simple Example
# 
# 
# https://learn.microsoft.com/en-us/azure/service-fabric/service-fabric-tutorial-deploy-api-management#microsoftapimanagementserviceapis
# 
# another way: https://learn.microsoft.com/en-us/fabric/onelake/onelake-access-python
# 
# 
# https://docs.python.org/3/howto/logging.html#logging-basic-tutorial
# 
# 
# 
# DEBUG
# 
# Detailed information, typically of interest only when diagnosing problems.
# 
# INFO
# 
# Confirmation that things are working as expected.
# 
# WARNING
# 
# An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
# 
# ERROR
# 
# Due to a more serious problem, the software has not been able to perform some function.
# 
# CRITICAL
# 
# A serious error, indicating that the program itself may be unable to continue running.
# 


# CELL ********************

import logging

log_path ='/lakehouse/default/Files/Log_Generic/'
file_name = 'Generic_test.log' 

logging.basicConfig(filename=log_path + file_name+'.txt',  
                                           force = True,
                                           filemode='w',
                                           level=logging.DEBUG)##or DEBUG


logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')
logging.error('Houston we have a problem')
logging.critical('OMG!!')
##This example also shows how you can set the logging level which acts as the threshold for tracking. In this case, because we set the threshold to DEBUG, all of the messages were printed.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
file_name = 'Generic_test_example_onerow.log_' 

logging.basicConfig(format='%(asctime)s %(message)s'
                    , filename=log_path +  file_name+'.txt'
                    ,force = True
                    ,filemode='w')
logging.warning('is when this event was logged.')




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logging.warning('is when this event was logged_config defined.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import logging
import time


  
#date for the name format
datestr = time.strftime("_%Y%m%d_T_%H%M")

#date for the log
datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")


log_path ='/lakehouse/default/Files/Log_Generic/'
file_name = 'divide_2_number.log_' + datestr

logging.basicConfig(filename=log_path + file_name+'.txt',  
                                           force = True,
                                           filemode='w',
                                           level=logging.INFO)##or DEBUG

#################### Begin of the function #################### 
def divide_2_number(a,b):
    try:
        result = a/b
        message = f"- {datestr_log}(UTC) - results are: {result} '\n"
        logging.info(message)
        print (message)
        return result
    except Exception as e:
        error_message = f"- {datestr_log}(UTC) - Exception occurred: '\n\n Follow the error:\n\n {e}\n"
        print(error_message)
        logging.critical(error_message)



  #################### End of the function#################### 


#execution of the function
divide_2_number(2,0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
