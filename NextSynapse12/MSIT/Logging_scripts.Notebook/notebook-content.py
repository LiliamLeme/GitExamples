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

pip install databricks-sql-connector

# MARKDOWN ********************

# #### Debugging connection between databricks and fabric

# CELL ********************

##logging and debug connection from Datbricks to Fabri
from databricks import sql
import os, logging

logging.getLogger("databricks.sql").setLevel(logging.DEBUG)
logging.basicConfig(filename = "results.log",
                    level    = logging.DEBUG,
                    path ='Files/Silver/' )

connection = sql.connect(
                        server_hostname = "adb-5700302634284155.15.azuredatabricks.net",
                        http_path = "/sql/1.0/warehouses/4126be26cf59b5df",
                        access_token = "dapib5493935b29a44b3abbec01a81076462")

cursor = connection.cursor()

cursor.execute("SELECT * from factinternetsales_delta limit 3")

result = cursor.fetchall()

for row in result:
   logging.debug(row)

cursor.close()
connection.close()
##https://realpython.com/python-logging/
##https://www.loggly.com/ultimate-guide/python-logging-basics/
##https://docs.python.org/3/howto/logging-cookbook.html


# CELL ********************

import logging
import time

datestr = time.strftime("d_%Y%m%d_h_%H%M")
#print (datestr)


log_path ='/lakehouse/default/Files/Silver/Logs/'
file_name = 'app1.log_' + datestr
logging.basicConfig(filename=log_path+file_name+'.txt', force = True
                    , filemode='w'
                    , format='%(name)s - %(levelname)s - %(message)s'
                    ,datefmt='%m-%d %H:%M'
                    ,level=logging.DEBUG)



logging.warning('This will get logged to a file')


#'/mnt/var/hadoop/tmp/nm-local-dir/usercache/trusted-service-user/appcache/application_1689261010958_0001/container_1689261010958_0001_01_000001/Files/Silver/app.log.txt'

#import logging
#from logging import FileHandler

# note, this will create a new logger if the name doesn't exist, 
# which will have no handlers attached (yet)


#mssparkutils.fs.cp('file:/tmp/app.log', 'Files/Silver/app.log')

##https://pypi.org/project/log4python/
##https://docs.python.org/3/howto/logging-cookbook.html


# CELL ********************

import logging


logging.basicConfig(filename='/lakehouse/default/Files/Silver/Log/App6.log.txt', force = True
                    , filemode='w'
                    , format='%(name)s - %(levelname)s - %(message)s'
                    ,datefmt='%m-%d %H:%M'
                    ,level=logging.DEBUG)



logging.debug('A DEBUG message')
logging.info('An INFO message')
logging.warning('A WARNING message')
logging.error('An ERROR message')
logging.critical('A CRITICAL message')


log = logging.getLogger("my-logger")
log.info("Hello, world")

# CELL ********************

pip install log4python

# CELL ********************

%pip install fire
#ModuleNotFoundError: No module named 'fire'
#https://google.github.io/python-fire/guide/

# CELL ********************

pip show fire

# CELL ********************

pip --version

# CELL ********************

pip install fire

#ModuleNotFoundError: No module named 'fire'
#https://google.github.io/python-fire/guide/

# CELL ********************

import fire

# CELL ********************

from log4python.Log4python import log
import fire

TestLog = log('/lakehouse/default/Files/Silver/Log/LogDemo')
#TestLog.debug("Debug Log")
#TestLog.info("Info Log")




# CELL ********************

hello('liliam')

# CELL ********************

##custom library.
from math_custom import add_2
                
print (add_2.add_2(1,2))

