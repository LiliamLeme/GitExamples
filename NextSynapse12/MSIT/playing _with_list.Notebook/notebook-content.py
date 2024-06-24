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
my_list = ['one','two']
type(my_list) type(my_list[1])


my_list = ['one','two']
my_list.extend(['four'])
my_new_list = my_list[:]
my_new_list

converted_list = tuple(my_list)


my_list = list(range(1, 4))
my_list


nums = [0, 1, 2, 3, 4]


square_roots = [math.sqrt(x) for x in nums] 

square_roots[0:2].pop()

# CELL ********************

pd.read_excel(open('tmp.xlsx', 'rb'),
              sheet_name='Sheet3')  

# CELL ********************

my_dict ={'key1':'value1', 'key2':'value2', 'key3':'value3'}
my_dict_set= set (my_dict)
my_dict_set_ans = 'value5' in my_dict_set
my_dict_set_ans
my_dict_set

##count words
string3 = 'This is a medium length string'
word_list = string3.lower().split(' ')

counter = 0


for word in word_list:
    if len(word)>3:
        counter+=1
    else:
        counter= counter
print (counter)



# CELL ********************

string1 = 'this string is assigned to a very cool variable'

string2 = string1.lower().split(' ')
type(string2)

# CELL ********************

my_dict ={'key1':'value1', 'key2':'value2', 'key3':'value3'}
my_dict_set= set (my_dict)
my_dict_set_ans = 'value5' in my_dict_set
my_dict_set_ans
my_dict_set

##count words
string3 = 'It was a bright day and it was a sunny day and some would say it was the perfect day to learn Python'

string3 = 'This is a medium length string'
word_list = string3.lower().split(' ')
word_list

counter = dict()


for word in word_list:
    if len(word_list)>3:
        counter[word]+=1
    else:
        counter[word]=1
print (counter)



for word in word_list:
    if word in counter:
        counter[word]+=1
    else:
        counter[word]=1
print (counter)


def count_words (text):
    charremo = list (",.!\'\"\n")
    text = text.replace ( '\n', ' ')
    for s in  charremo:
        text =text.replace (s, '')
    text=text.lower()
    token = text.split (' ')
    counter = dict()
    for w in token:
        counter[w] = counter.get(w,0)+1
    return

count_words(string3)

# CELL ********************


def count_words (text):
    charremo = list (",.!\'\"\n")
    text = text.replace ( '\n', ' ')
    for s in  charremo:
        text =text.replace (s, '')
    text=text.lower()
    token = text.split (' ')
    counter = dict()
    for w in token:
        counter[w] = counter.get(w,0)+1
    return

count_words(string3)

# CELL ********************


text = 'ABC DE'

text.replace = text('\n', ' ')

text

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

# Welcome to your new notebook
# Type here in the cell editor to add code!

my_list = ['one','two']
for a in my_list:
    print(f"{a.title()}, mississippi!")

# CELL ********************

list_calc=[]
for value in range (1,6):
    result = value ** 2 #power of 2
    list_calc.append(result)
    minimo = min(list_calc)
    Maximo = max(list_calc)

print(list_calc)
print (f"smaller is: {minimo}")
print (f"greater is: {Maximo}")

# CELL ********************

#another way - list of compreension
list_calc=[value ** 2 for value in range (1,11)]
print (list_calc) 
print (list_calc[0:5]) #slicing

# CELL ********************

list_calc=[value ** 2 for value in range (1,11)]
list_calc1= list_calc[:] #copying the list
for a in  list_calc1[0:5]:
    if divmod(a,3) ==(3,0):
        print (f"can be divided by 3 {a}\n")
    else:
        print (f" it cannot be divided by 3 {a} \n")

# CELL ********************

divmod(12,3)

# CELL ********************

#kind of a mini dictionary
dic = {'color': 'green', 'person': 'me' }
print (dic['color'])


print ("===========")
for key,value in dic.items():#looping only
    print (f" Some {key.title()}")
    print (f" Some {value.title()} \n")

print ("===========")
for value in sorted(dic.keys()): #looping in a sorted way
    print (f" Some {value.title()} \n")

    
print ("===========")
for a in sorted(dic.values()): #looping in a sorted way
    print (f" Some {a.title()} \n")



# CELL ********************



import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv"
df = pd.read_csv("/lakehouse/default/" + "Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv")
#display(df)

df1= df[(df['SalesReasonKey']>0)]
df1['SalesReasonKey'].head()

mino = min(df1['SalesReasonKey'])
maxo= max(df1['SalesReasonKey'])

print (f" min is {mino}  and max is {maxo}")


# CELL ********************

#getting the min and max values for that column - ##possibility creating a watermark
import csv
from datetime import datetime

#date for the name format
#datestr = time.strftime("_%Y%m%d_T_%H%M")
#date for the log
#datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")
#mssparkutils.fs.cp('/sqlserverlessanalitics/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')
#mssparkutils.fs.cp('/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')

file_dict_csv = dict ()
#creating a dictionary 
with open ("/lakehouse/default/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv") as csvfile :
    file_dict_csv = list (csv.DictReader(csvfile)) 

for value in file_dict_csv['SalesReasonKey']:
    print (f"\t {value}")


# CELL ********************

import timeit

print("The time taken is ",timeit.timeit)

# CELL ********************

#creating a list from a columns
#getting the min and max values for that column - ##possibility creating a watermark
import csv
from datetime import datetime
import datetime
%timeit -n 2


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

#day = time.strftime("%Y%m%d")
#Dictionaries are used to store data values in key:value pairs. A dictionary is a collection which is ordered*, changeable and do not allow duplicates. Dictionaries are written with curly brackets, and have keys and values


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

# CELL ********************

mssparkutils.fs.cp('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/HumanResources.Employee/part-00000-0a3c54ce-93dd-4196-aaf4-7998a455c349-c000.snappy.parquet', 'file:/tmp/temp/HumanResources.Employee/part-00000-f88db24d-098f-4872-a525-2012fd9bc2ba-c000.snappy.parquet')
mssparkutils.fs.cp('abfss://NextSynapse12@msit-onelake.pbidedicated.windows.net/SQLDW.Lakehouse/Files/Files/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv', 'file:/tmp/temp/DimSalesReason/part-00000-7decee81-2746-4ad4-a113-7b829e88f2b5-c000.csv')

