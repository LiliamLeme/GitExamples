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

# CELL ********************

from databricks import sql
import os
from pandas import DataFrame
from IPython.display import display

spark.conf.set("sprk.sql.parquet.vorder.enabled", "true") # Enable VOrder write
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true") # Enable automatic delta optimized write
#https://docs.delta.io/latest/optimizations-oss.html#language-python
#https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order?tabs=sparksql

connection = sql.connect(
                        server_hostname = "adb-5700302634284155.15.azuredatabricks.net",
                        http_path = "/sql/1.0/warehouses/4126be26cf59b5df",
                        access_token = "dapib5493935b29a44b3abbec01a81076462")

cursor = connection.cursor()

#cursor.execute("SELECT  * from factinternetsales_delta limit 2")
#print(cursor.fetchall())

query = cursor.execute(f"SELECT * FROM factinternetsales_delta")
#print(f"Query output: SELECT * FROM factinternetsales_delta LIMIT 10\n")
#for row in cursor.fetchall():
#  print( str(row)+ " \n") 


df = DataFrame(query.fetchall())
#print(df.head())
display(df)

spark_df = spark.createDataFrame(df)

spark_df.write.format("delta").mode("overwrite").saveAsTable("factinternetsales_delta")



cursor.close()
connection.close()
#https://docs.databricks.com/dev-tools/python-sql-connector.html
#toeken from Databricks -> Connection details -> Python connector.

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


logging.basicConfig(filename='/lakehouse/default/Files/Silver/Log/app1.log.txt', force = True
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


logging.basicConfig(filename='app2.log.json', force = True
                    , filemode='w'
                    , format='%(name)s - %(levelname)s - %(message)s'
                    ,datefmt='%m-%d %H:%M'
                    ,level=logging.DEBUG)


import json
import logging
import logging.config

CONFIG = '''
{
    "version": 1,
    "disable_existing_loggers": false,
    "formatters": {
        "simple": {
            "format": "%(levelname)-8s - %(message)s"
        }
    },
    "filters": {
        "warnings_and_below": {
            "()" : "__main__.filter_maker",
            "level": "WARNING"
        }
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "simple",
            "stream": "ext://sys.stdout",
            "filters": ["warnings_and_below"]
        },
        "stderr": {
            "class": "logging.StreamHandler",
            "level": "ERROR",
            "formatter": "simple",
            "stream": "ext://sys.stderr"
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "simple",
            "filename": "app.log",
            "mode": "w"
        }
    },
    "root": {
        "level": "DEBUG",
        "handlers": [
            "stderr",
            "stdout",
            "file"
        ]
    }
}
'''

def filter_maker(level):
    level = getattr(logging, level)

    def filter(record):
        return record.levelno <= level

    return filter
logging.config.dictConfig(json.loads(CONFIG))
logging.debug('A DEBUG message')
logging.info('An INFO message')
logging.warning('A WARNING message')
logging.error('An ERROR message')
logging.critical('A CRITICAL message')

# CELL ********************

df = spark.read.text("Files/Silver/Log/app1.log.txt")
# df now is a Spark DataFrame containing text data from "Files/Silver/Log/app1.log.txt".
display(df)
