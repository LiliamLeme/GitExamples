# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "67f8983e-c811-4672-9b76-77704bf6075a",
# META       "default_lakehouse_name": "SQLDW",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
#import os
#import traceback
import logging
import time

#date for the name format
datestr = time.strftime("_%Y%m%d_T_%H%M")
#date for the log
datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")
#print (datestr)
#https://docs.python.org/3/howto/logging-cookbook.html#formatting-times-using-utc-gmt-via-configuration

log_path ='/lakehouse/default/Files/Files/Log_Generic/'
file_name = 'divide_2_number.log_' + datestr
logging.basicConfig(filename=log_path + file_name+'.txt',  force = True
                    , filemode='w'
                    #, format='%(name)s - %(levelname)s - %(message)s'
                    #,datefmt='%m-%d %H:%M'
                    ,level=logging.INFO)##or DEBUG

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
        #traceback_info= 'Follow the traceback: \n' + traceback.format_exc() + "\n"
        #error_message += traceback_info
        logging.critical(error_message)
    
 
divide_2_number(2,0)

# CELL ********************

divmod(12,3)

# CELL ********************

#creating a list from a columns
#getting the min and max values for that column - ##possibility creating a watermark
import csv
from datetime import datetime

#date for the name format
#datestr = time.strftime("_%Y%m%d_T_%H%M")
#date for the log
#datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")
#mssparkutils.fs.cp('/sqlserverlessanalitics/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')
#mssparkutils.fs.cp('/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')

#creating a dictionary 
with open ("/lakehouse/default/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv") as csvfile :
    file_dict_csv = list (csv.DictReader(csvfile)) 
file_dict_csv[:5]

#list_value= []
#list_value = list(file_dict_csv['SalesReasonKey']

##defining my list size as my index must be an integer so I can properly filter the slice.
size = len(list(file_dict_csv) )
n=0
#list_value= []
list_value= []
for value in list(file_dict_csv):
    list_value.append(file_dict_csv[n]['SalesReasonKey'])
    if n < size:
       n=n+1
    print (f"\t {list_value}")

    
mino = min(list_value)
maxo= max(list_value)

print (f" min is {mino}  and max is {maxo}")

day = time.strftime("%Y%m%d")


# MARKDOWN ********************

# ### Profilling ad check performance


# CELL ********************

#creating a list from a columns
#getting the min and max values for that column - ##possibility creating a watermark
import csv
from datetime import datetime
import functools
import timeit
import cProfile
import re
import profile

#date for the name format
#datestr = time.strftime("_%Y%m%d_T_%H%M")
#date for the log
#datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")
#mssparkutils.fs.cp('/sqlserverlessanalitics/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')
#mssparkutils.fs.cp('/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')

#creating a dictionary 
with open ("/lakehouse/default/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv") as csvfile :
    file_dict_csv = list (csv.DictReader(csvfile)) 
file_dict_csv[:5]

#list_value= []
#list_value = list(file_dict_csv['SalesReasonKey']




@functools.lru_cache(maxsize=128)
def create_list_SalesReasonKey2():
##defining my list size as my index must be an integer so I can properly filter the slice.
    size = len(list(file_dict_csv) )
    n=0
    #list_value= []
    list_value= []
    for value in list(file_dict_csv):
        list_value.append(file_dict_csv[n]['SalesReasonKey'])
        if n < size:
            n=n+1
        print (f"\t {list_value}")
    return list_value


##timeit
start_time = timeit.default_timer()

print("The start time is with function :",
             start_time)
             

##function execution
list_value=create_list_SalesReasonKey2()

##timeit results
print("The current is with function :",
             timeit.default_timer() - start_time)

print("The difference of time is with function :",
             timeit.default_timer() - start_time)

#import profile##https://wiki.python.org/moin/PythonSpeed/PerformanceTips
profile.run('create_list_SalesReasonKey2()')



#@functools.lru_cache(maxsize=128)
def create_list_SalesReasonKey():
##defining my list size as my index must be an integer so I can properly filter the slice.
    size = len(list(file_dict_csv) )
    n=0
    #list_value= []
    list_value= []
    for value in list(file_dict_csv):
        list_value.append(file_dict_csv[n]['SalesReasonKey'])
        if n < size:
            n=n+1
        print (f"\t {list_value}")
    return list_value
#create_list_SalesReasonKey()


##profile
cProfile.run('re.compile("create_list_SalesReasonKey()")')

##timeit
start_time = timeit.default_timer()

print("The start time is :",
             start_time)

##function execution
list_value=create_list_SalesReasonKey()

##timeit results
print("The current is :",
             timeit.default_timer() - start_time)

print("The difference of time is :",
             timeit.default_timer() - start_time)






mino = min(list_value)
maxo= max(list_value)

print (f" min is {mino}  and max is {maxo}")




# CELL ********************


%%sql
--Create a Table naming as sampleTable under CT database.
CREATE TABLE watermark_dimssalesreason (min_value int, max_value int, day string)


--spark.sql(f"INSERT INTO watermark_dimssalesreason ({mino}, {maxo}, {day})")

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

#%%sql
#DROP TABLE watermark_dimssalesreason

# METADATA ********************

# META {}

# CELL ********************

spark.sql(f"INSERT INTO watermark_dimssalesreason VALUES ({mino}, {maxo}, {day})")

# METADATA ********************

# META {}

# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.watermark_dimssalesreason LIMIT 1000")
display(df)
