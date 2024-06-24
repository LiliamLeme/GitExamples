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
# META         },
# META         {
# META           "id": "93b443f5-777a-4196-9e74-d4aea2ca4700"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

##reading product from adventureworks exported into parquet mode
mssparkutils.fs.cp('Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')


# MARKDOWN ********************

# ### Creating the Dictionary

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import csv
#mssparkutils.fs.cp('/folderonthestorage/filename.csv', 'file:/tmp/temp/filename.csv')

with open ("/tmp/temp/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv") as csvfile :
    file_dict_csv = list (csv.DictReader(csvfile)) 
#showing first 5 
file_dict_csv[:5]
