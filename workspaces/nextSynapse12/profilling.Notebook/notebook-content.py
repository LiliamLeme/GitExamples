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
#creating a list from a columns
#getting the min and max values for that column - ##possibility creating a watermark
import csv
from datetime import datetime
import functools
import timeit
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



##function execution
list_value=create_list_SalesReasonKey2()


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



##function execution
list_value=create_list_SalesReasonKey()


##profile
profile.run('create_list_SalesReasonKey()')






mino = min(list_value)
maxo= max(list_value)

print (f" min is {mino}  and max is {maxo}")



