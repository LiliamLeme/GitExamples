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

# ### Copy Data Recursively from Shortcut of one Workspace to other.


# CELL ********************

##https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/microsoft-spark-utilities?pivots=programming-language-python
files = mssparkutils.fs.ls('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/')
for file in files:
    #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
    bisDir = file.isDir
    vfolder = file.name
    print (bisDir)
    if bisDir == 1:
        files_folder = mssparkutils.fs.ls(f'abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/{vfolder}/')
        print (files_folder)
        for file in files_folder:
            #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
            vfile = file.name
            print (vfile)
            
            vpathsource = f'abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/{vfolder}/{vfile}'
            vpathdestiny = f'abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/{vfolder}/{vfile}'
            mssparkutils.fs.cp(vpathsource, vpathdestiny)

    ##reading product from adventureworks exported into parquet mode
#mssparkutils.fs.cp('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Data_health_MaidH/2020/*.csv', 'abfss://Fabric12@msit-onelake.dfs.fabric.microsoft.com/Health.Lakehouse/Files/Data_health/2020/trans25aug20.csv')



# CELL ********************

import os
import zipfile

vpathsource = 'abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/'
vpathdestiny = 'abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/Cultural_Infra/'

# List files and directories in vpathsource
files_and_dirs = mssparkutils.fs.ls(str(vpathsource))

for item in files_and_dirs:
    if item.isFile:
        # If it's a file, copy it to the destination
        vpathsource_file = os.path.join(vpathsource, item.name)
        vpathdestiny_file = os.path.join(vpathdestiny, item.name)
        mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)
    elif item.isDir:
        # If it's a directory, create a corresponding directory in the destination
        vfolder = item.name
        vpathsource_dir = os.path.join(vpathsource, vfolder)
        vpathdestiny_dir = os.path.join(vpathdestiny, vfolder)
        mssparkutils.fs.mkdirs(vpathdestiny_dir)

        # List files in the source directory
        files_in_dir = mssparkutils.fs.ls(str(vpathsource_dir))
        for file_in_dir in files_in_dir:
            if file_in_dir.isFile:
                # Copy files from the source directory to the destination directory
                vfile = file_in_dir.name
                vpathsource_file = os.path.join(vpathsource_dir, vfile)
                vpathdestiny_file = os.path.join(vpathdestiny_dir, vfile)
                mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)
                       ##zip file



# MARKDOWN ********************

# ### Zip library

# CELL ********************

import zipfile                       
with zipfile.ZipFile('/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries_copy2.zip', 'r') as zip_ref:
     zip_ref.extractall('/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries_copy2.csv')

# MARKDOWN ********************

# ### Zip library with Mssparkutils


# CELL ********************

import os
import zipfile
from concurrent.futures import ThreadPoolExecutor


##abfss path as mssparkuilts does not accept API path
vpathsource = 'abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/'
vpathdestiny = 'abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/Cultural_Infra/'

##API onelake path for zipfile library usage
vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/'


##Function is a zip? unzip
##extension = '.csv'
def is_aZip(vpathdestiny_APIpath, item, extension):
    vpathdestiny_APIpath_name = os.path.join(vpathdestiny_APIpath, item)
    _, ext = os.path.splitext(vpathdestiny_APIpath_name)

    if ext.lower() == '.zip':
        item_new = os.path.splitext(item)[0] + extension

        with zipfile.ZipFile(vpathdestiny_APIpath_name, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(vpathdestiny_APIpath, item_new))


##vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/'
##is_aZip(vpathdestiny_APIpath, 'kardex.zip')

    # List files and directories in vpathsource
    files_and_dirs = mssparkutils.fs.ls(str(vpathsource))

    ##if the item is a file -> copy
    for item in files_and_dirs:
        if item.isFile:
            # If it's a file, copy it to the destination
            vpathsource_file = os.path.join(vpathsource, item.name)
            vpathdestiny_file = os.path.join(vpathdestiny, item.name)
            mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)
            
            ##it must be API path, not ABFSS. So the current will concat to the API path to be able to use ZIP library
            folder_name = os.path.basename(os.path.dirname(vpathdestiny_file))
            vpathdestiny_APIpath_folder = vpathdestiny_APIpath+ '/' +folder_name + '/' 
            extension = '.csv'
                
            is_aZip(vpathdestiny_APIpath_folder, vfile, extension)


        ##if it is a directory go recrusevely to the file. 
        elif item.isDir:
            # If it's a directory, create a corresponding directory in the destination
            vfolder = item.name
            vpathsource_dir = os.path.join(vpathsource, vfolder)
            vpathdestiny_dir = os.path.join(vpathdestiny, vfolder)
            mssparkutils.fs.mkdirs(vpathdestiny_dir)

        


            # List files in the source directory
            files_in_dir = mssparkutils.fs.ls(str(vpathsource_dir))
            for file_in_dir in files_in_dir:
                if file_in_dir.isFile:
                    # Copy files from the source directory to the destination directory
                    vfile = file_in_dir.name
                    vpathsource_file = os.path.join(vpathsource_dir, vfile)
                    vpathdestiny_file = os.path.join(vpathdestiny_dir, vfile)
                    mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)
                        ##zip file
                    
                    ##it must be API path, not ABFSS. So the current will concat to the API path to be able to use ZIP library
                    folder_name = os.path.basename(os.path.dirname(vpathdestiny_file))
                    vpathdestiny_APIpath_folder = vpathdestiny_APIpath + '/' + folder_name + '/' 
                    extension = '.csv'
                
                is_aZip(vpathdestiny_APIpath_folder, vfile, extension)

        with ThreadPoolExecutor() as executor:
            executor.submit(copy)


# CELL ********************


import os
import zipfile
import concurrent.futures


##abfss path as mssparkuilts does not accept API path
vpathsource = 'abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/'
vpathdestiny = 'abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/Cultural_Infra/'

##API onelake path for zipfile library usage
vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/'


##Function is a zip? unzip
##extension = '.csv'
def is_aZip(vpathdestiny_APIpath, item, extension):
    vpathdestiny_APIpath_name = os.path.join(vpathdestiny_APIpath, item)
    _, ext = os.path.splitext(vpathdestiny_APIpath_name)

    if ext.lower() == '.zip':
        item_new = os.path.splitext(item)[0] + extension

        with zipfile.ZipFile(vpathdestiny_APIpath_name, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(vpathdestiny_APIpath, item_new))


##vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/'
##is_aZip(vpathdestiny_APIpath, 'kardex.zip')

   

def process_item(item, vpathsource, vpathdestiny, vpathdestiny_APIpath):
     # List files and directories in vpathsource
    files_and_dirs = mssparkutils.fs.ls(str(vpathsource))
    if item.isFile:
        # If it's a file, copy it to the destination
        vpathsource_file = os.path.join(vpathsource, item.name)
        vpathdestiny_file = os.path.join(vpathdestiny, item.name)
        mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)

        # It must be an API path, not ABFSS.
        folder_name = os.path.basename(os.path.dirname(vpathdestiny_file))
        vpathdestiny_APIpath_folder = vpathdestiny_APIpath + '/' + folder_name + '/'
        extension = '.csv'

        process_zipfile(vpathdestiny_APIpath_folder, item.name, extension)

    elif item.isDir:
        # If it's a directory, create a corresponding directory in the destination
        vfolder = item.name
        vpathsource_dir = os.path.join(vpathsource, vfolder)
        vpathdestiny_dir = os.path.join(vpathdestiny, vfolder)
        mssparkutils.fs.mkdirs(vpathdestiny_dir)

        # List files in the source directory
        files_and_dirs = mssparkutils.fs.ls(str(vpathsource_dir))
    

        # Use ThreadPoolExecutor to parallelize the processing of files in the directory
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_zipfile, vpathdestiny_APIpath + '/' + vfolder + '/', file_in_dir.name, '.csv') for file_in_dir in files_in_dir if file_in_dir.isFile]

            # Wait for all futures to complete
            concurrent.futures.wait(futures)

# Assuming you have vpathsource, vpathdestiny, and vpathdestiny_APIpath defined somewhere
files_and_dirs =  mssparkutils.fs.ls(str(vpathsource)) 

# Process each item (file or directory) in parallel using ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(process_item, item, vpathsource, vpathdestiny, vpathdestiny_APIpath) for item in files_and_dirs]
    print (futures)
    print (files_and_dirs)
    # Wait for all futures to complete
    concurrent.futures.wait(futures)

# CELL ********************

for file in files:    
        source_path = f"{mssparkutils.fs.getMountPath('/test')}/{file}"
        destination_path = '/lakehouse/default/Files/Python_POC/Extracted_Batch_Logging/'
        with ThreadPoolExecutor() as executor:
            executor.submit(extract_zip, source_path, destination_path, file)

# MARKDOWN ********************

# ### ZIP with file and Logging

# CELL ********************

vpathdestiny = 'abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/Cultural_Infra/'
mssparkutils.fs.ls(str(vpathdestiny))

# CELL ********************

##vpathdestiny_APIpath= '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/'
##mssparkutils.fs.ls(str(vpathdestiny))


mssparkutils.fs.cp('/lakehouse/default/Files/Log_Parallel/Generic_test_example_onerow.log_.txt', '/lakehouse/default/Files/Log_Generic/Generic_test_example_onerow.log_.txt')

mssparkutils.fs.cp('/lakehouse/default/Files/Log_Parallel/Generic_test_example_onerow.log_.txt', '/lakehouse/default/Files/Log_Generic/Generic_test_example_onerow.log_.txt')



# CELL ********************

import os
import zipfile
import logging
import time

##abfss path as mssparkuilts does not accept API path
vpathsource = 'abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/'
vpathdestiny = 'abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/Cultural_Infra/'

##API onelake path for zipfile library usage
vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/'

#######################################################################
##log configuration
#######################################################################

  
#date for the name format
datestr = time.strftime("_%Y%m%d_T_%H%M")

#date for the log
datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")

log_path ='/lakehouse/default/Files/Log_Generic/'

file_name = 'copy_between_paths.log_' + datestr




logging.basicConfig(filename=log_path + file_name+'.txt',  
                                           force = True,
                                           filemode='w',
                                           level=logging.INFO)##or DEBUG

#######################################################################
##Zip or Unzip file
#######################################################################

##Function is a zip? unzip
##extension = '.csv'
def is_aZip(vpathdestiny_APIpath, item, extension):
    vpathdestiny_APIpath_name = os.path.join(vpathdestiny_APIpath, item)
    _, ext = os.path.splitext(vpathdestiny_APIpath_name)

    if ext.lower() == '.zip':
        item_new = os.path.splitext(item)[0] + extension

        with zipfile.ZipFile(vpathdestiny_APIpath_name, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(vpathdestiny_APIpath, item_new))

##example
##vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/'
##is_aZip(vpathdestiny_APIpath, 'kardex.zip')

##################################
###Copy files
##################################



def copy_betweenpaths(vpathsource,vpathdestiny):
    # List files and directories in vpathsource
    files_and_dirs = mssparkutils.fs.ls(str(vpathsource))
    try:

        ##if the item is a file -> copy
        for item in files_and_dirs:
            if item.isFile:
                # If it's a file, copy it to the destination
                vpathsource_file = os.path.join(vpathsource, item.name)
                vpathdestiny_file = os.path.join(vpathdestiny, item.name)
                mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)
                
                ##it must be API path, not ABFSS. So the current will concat to the API path to be able to use ZIP library
                folder_name = os.path.basename(os.path.dirname(vpathdestiny_file))
                vpathdestiny_APIpath_folder = vpathdestiny_APIpath+ '/' +folder_name + '/' 
                extension = '.csv'
                    
                is_aZip(vpathdestiny_APIpath_folder, vfile, extension)


                message = f"- {datestr_log}(UTC) - copy from {vpathsource} to \n\n {vpathdestiny} \n"
                logging.info(error_message)

            ##if it is a directory go recrusevely to the file. 
            elif item.isDir:
                # If it's a directory, create a corresponding directory in the destination
                vfolder = item.name
                vpathsource_dir = os.path.join(vpathsource, vfolder)
                vpathdestiny_dir = os.path.join(vpathdestiny, vfolder)
                mssparkutils.fs.mkdirs(vpathdestiny_dir)

             
                message = f"- {datestr_log}(UTC) - copy from {vpathsource} to \n\n {vpathdestiny} \n"
                logging.info(error_message)

                # List files in the source directory
                files_in_dir = mssparkutils.fs.ls(str(vpathsource_dir))
                for file_in_dir in files_in_dir:
                    if file_in_dir.isFile:
                        # Copy files from the source directory to the destination directory
                        vfile = file_in_dir.name
                        vpathsource_file = os.path.join(vpathsource_dir, vfile)
                        vpathdestiny_file = os.path.join(vpathdestiny_dir, vfile)
                        mssparkutils.fs.cp(vpathsource_file, vpathdestiny_file)
                            ##zip file
                        
                        ##it must be API path, not ABFSS. So the current will concat to the API path to be able to use ZIP library
                        folder_name = os.path.basename(os.path.dirname(vpathdestiny_file))
                        vpathdestiny_APIpath_folder = vpathdestiny_APIpath + '/' + folder_name + '/' 
                        extension = '.csv'
                    
                        is_aZip(vpathdestiny_APIpath_folder, vfile, extension)

                        message = f"- {datestr_log}(UTC) - copy from {vpathsource} to \n\n {vpathdestiny} \n"
                        logging.info(error_message)

    except Exception as e:
       error_message = f"- {datestr_log}(UTC) - Exception occurred: '\n\n Follow the error:\n\n {e}\n"
       #print(error_message)
       logging.critical(error_message)

##################################
###Execute Function
##################################

copy_betweenpaths(vpathsource,vpathdestiny)

# CELL ********************

import os
import zipfile

def is_aZip(vpathdestiny_APIpath, item):
    vpathdestiny_APIpath_name = os.path.join(vpathdestiny_APIpath, item)
    _, ext = os.path.splitext(vpathdestiny_APIpath_name)

    if ext.lower() == '.zip':
        item_new = os.path.splitext(item)[0] + '.csv'

        with zipfile.ZipFile(vpathdestiny_APIpath_name, 'r') as zip_ref:
            zip_ref.extractall(os.path.join(vpathdestiny_APIpath, item_new))

# Example usage
vpathdestiny_APIpath = '/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/'
is_aZip(vpathdestiny_APIpath, 'kardex.zip')




# MARKDOWN ********************

# ### Copy the data recursively  and logging

# CELL ********************

import os
import logging
import time

  
#date for the name format
datestr = time.strftime("_%Y%m%d_T_%H%M")

#date for the log
datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")


#mssparkutils.fs.mkdirs('abfss:/Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Log_Generic/') ##create folder if does not exist

log_path ='/lakehouse/default/Files/Log_Generic/'

psource ='abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/'
pdestiny ='abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/'
file_name = 'copy_between_paths.log_' + datestr



logging.basicConfig(filename=log_path + file_name+'.txt',  
                                           force = True,
                                           filemode='w',
                                           level=logging.INFO)##or DEBUG




#################### Begin of the function #################### 
def copy_betweenpaths(psource,pdestiny):
    try:
        files = mssparkutils.fs.ls(psource)
        for file in files:
            #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
            bisDir = file.isDir
            vfolder = file.name
            print (bisDir)
            if bisDir == 1:
                pfiles_folder  = psource + vfolder + '/'
                #print (pfiles_folder)
                files_folder = mssparkutils.fs.ls(pfiles_folder)
                print (files_folder)
                for file in files_folder:
                    #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
                    vfile = file.name
                    #print (vfile)
                    vpathsource = psource + vfolder +'/' + vfile
                    vpathdestiny = pdestiny + vfolder + '/' + vfile
                    #print (vpathsource)
                    #print (vpathdestiny)
                    mssparkutils.fs.cp(vpathsource, vpathdestiny)
                    message = f"- {datestr_log}(UTC) - copy from {vpathsource} to \n\n {vpathdestiny} \n"
                    logging.info(error_message)
    except Exception as e:
        error_message = f"- {datestr_log}(UTC) - Exception occurred: '\n\n Follow the error:\n\n {e}\n"
        #print(error_message)
        logging.critical(error_message)



  #################### End of the function#################### 


#execution of the function
copy_betweenpaths(psource,pdestiny)

# CELL ********************

import os
import logging
import time
import zipfile

  
#date for the name format
datestr = time.strftime("_%Y%m%d_T_%H%M")

#date for the log
datestr_log = time.strftime("%Y-%m-%d - %H:%M:%S:%M")


#mssparkutils.fs.mkdirs('abfss:/Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Log_Generic/') ##create folder if does not exist

log_path ='/lakehouse/default/Files/Log_Generic/'

psource ='abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/Cultural_Infra/'
pdestiny ='abfss://Airlift_Demo@msit-onelake.dfs.fabric.microsoft.com/Airlift_LH.Lakehouse/Files/Cultural_Infra/'
file_name = 'copy_between_paths.log_' + datestr



logging.basicConfig(filename=log_path + file_name+'.txt',  
                                           force = True,
                                           filemode='w',
                                           level=logging.INFO)##or DEBUG




#################### Begin of the function #################### 
def copy_betweenpaths(psource,pdestiny):
    try:
        files = mssparkutils.fs.ls(psource)
        for file in files:
            print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
            bisDir = file.isDir
            vfolder = file.name
            print (bisDir)
            if bisDir == 1:
                pfiles_folder  = psource + vfolder + '/'
                print (pfiles_folder)
                files_folder = mssparkutils.fs.ls(pfiles_folder)
                print (files_folder)
                for file in files_folder:
                    #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
                    vfile = file.name
                    print (vfile)
                    vpathsource = psource + vfolder +'/' + vfile
                    vpathdestiny = pdestiny + vfolder + '/' + vfile
                    #print (vpathsource)
                    print (vpathdestiny)
                    mssparkutils.fs.cp(vpathsource, vpathdestiny)
                    message = f"- {datestr_log}(UTC) - copy from {vpathsource} to \n\n {vpathdestiny} \n"
                    logging.info(error_message)

                    ##zip file
                    with zipfile.ZipFile(vpathdestiny, 'r') as zip_ref:
                         zip_ref.extractall(vpathdestiny)



    except Exception as e:
        error_message = f"- {datestr_log}(UTC) - Exception occurred: '\n\n Follow the error:\n\n {e}\n"
        #print(error_message)
        logging.critical(error_message)



  #################### End of the function#################### 


#execution of the function
copy_betweenpaths(psource,pdestiny)

# CELL ********************

#https://data.london.gov.uk/dataset/travel-work-bicycle-ward
#https://s3-eu-west-1.amazonaws.com/londondatastore-upload/technical-note-04-how-has-cycling-grown-in-london.pdf

#https://data.london.gov.uk/dataset/cultural-infrastructure-map


# MARKDOWN ********************

# ### Create tables from the data copied.

# CELL ********************

##create tables
##csv to parquet
#read and evaluate.

##https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-notebook-load-data

import os

files = mssparkutils.fs.ls('Files/Cultural_Infra/')
for file in files:
    #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
    bisDir = file.isDir
    vfolder = file.name
    if bisDir == 1:
        files_folder = mssparkutils.fs.ls(f'Files/Cultural_Infra/{vfolder}/')
        for file in files_folder:
            #print(file.name, file.isDir, file.isFile, file.path, file.size, file.modifyTime)
            vfile = file.name
            print (vfile)
            file_name_without_extension =  os.path.splitext(vfile)[0]
       
            new_file_name = file_name_without_extension.replace(" ", "_")
            df =  spark.read.format("csv").option("header","true")\
                            .load(f'Files/Cultural_Infra/{vfolder}/{vfile}')
                            
            df.write.mode("overwrite").format("delta").saveAsTable(f'{new_file_name}')
   






# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Cultural_Infra/Cultural_Infra/Dance/CIM 2023 Dance performance venues.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Cultural_Infra/Cultural_Infra/Dance/CIM 2023 Dance performance venues.csv".
display(df)

# CELL ********************

# MAGIC %%sql
# MAGIC OPTIMIZE cim_2023_music_all ZORDER BY (ward_2022_name)


# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# MARKDOWN ********************

# #### Min and max venue per ward

# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt

df_music = spark.sql("SELECT * FROM cim_2023_music_all   ")


df_music_pandas = df_music.toPandas()
df_music_pandas.head()

# Count the number of venues per ward
venue_count = df_music_pandas["ward_2022_name"].value_counts()

# Find the maximum and minimum number of venues per ward
max_venues = venue_count.max()
min_venues = venue_count.min()

# Plot the results
plt.figure(figsize=(10, 6))
plt.bar(venue_count.index, venue_count.values)
plt.xlabel("Ward (Neighborhood)")
plt.ylabel("Number of Venues")
plt.title("Number of Music Venues per Ward in London")
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

# Display the maximum and minimum number of venues
print(f"Max Venues per Ward: {max_venues}")
print(f"Min Venues per Ward: {min_venues}")


# MARKDOWN ********************

# ### STD, MIn, MAx

# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df_music = spark.sql("SELECT * FROM cim_2023_music_all  ")
##creating a pandas dataframe from the results
df_music = df_music.toPandas()

## how much events per venue
venue_count = df_music["ward_2022_name"].value_counts()

# Calculate the standard deviation, min, max, mean of the number of venues per ward
std_deviation = venue_count.std()
mean_events = venue_count.mean()
max_venues = venue_count.max()
min_venues = venue_count.min()

# print the results
print(f"Standard Deviation of Music Events: {std_deviation:.2f}")
print(f"Mean of Music Events: {mean_events:.2f}")
print(f"Max Venues per Ward: {max_venues}")
print(f"Min Venues per Ward: {min_venues}")


# Create a histogram of the number of events per ward using seaborn's histplot
sns.histplot(venue_count, bins=30, kde=True)

# Add title and labels
plt.title('Histogram of Music Events per Ward in London')
plt.xlabel('Number of Events')
plt.ylabel('Frequency')

# Outliers
mean_events = df_music["ward_2022_name"].value_counts().mean()
outliers = df_music["ward_2022_name"].value_counts()[(df_music["ward_2022_name"].value_counts() > (mean_events + 1.5 * std_deviation))]
print("Wards with Outliers:")
print(outliers)


# MARKDOWN ********************

# ### TOP 10 values on the Histogram

# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt


# Count the number of venues per ward
venue_count = df_music["ward_2022_name"].value_counts()

# Find the maximum and minimum number of venues per ward
max_venues = venue_count.max()
min_venues = venue_count.min()

# List the top 10 wards with the most music events
top_10_wards = venue_count.head(10)

# Plot the results
plt.figure(figsize=(10, 6))
plt.bar(venue_count.index, venue_count.values)
plt.xlabel("Ward (Neighborhood)")
plt.ylabel("Number of Venues")
plt.title("Number of Music Venues per Ward in London")
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

# Display the maximum and minimum number of venues
print(f"Max Venues per Ward: {max_venues}")
print(f"Min Venues per Ward: {min_venues}")

# Display the top 10 wards with the most music events
print("Top 10 Wards with the Most Music Events:")
print(top_10_wards)


# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt


df_music = spark.sql("SELECT * FROM cim_2023_music_all  ")
##creating a pandas dataframe from the results
df_music = df_music.toPandas()

# Count the number of venues per ward for each dataset
grassroots_venue_count = df_music_pandas["ward_2022_name"].value_counts()
recording_venue_count = df_music_recordingc_pandas["ward_2022_name"].value_counts()

# Merge the two DataFrames on the ward column
merged_df = pd.merge(grassroots_venue_count, recording_venue_count, left_index=True, right_index=True, how="outer")
merged_df = merged_df.fillna(0)  # Fill NaN values with 0

# Rename columns for clarity
merged_df.columns = ["music_rehearsal", "Recording Venues"]

# Plot the relationship between grassroots and recording venues
merged_df.plot(kind="scatter", x="music_rehearsal", y="Recording Venues")
plt.xlabel("Number of music_rehearsal")
plt.ylabel("Number of Recording Venues")
plt.title("Relationship between Rehearsal and Recording Venues by Ward")
plt.show()


# CELL ********************

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Assuming you have already executed your Spark SQL queries and stored the results
df_rehearsal = spark.sql("SELECT * FROM cim_2023_music_rehearsal_studios LIMIT 1000").toPandas()
df_recording_studios = spark.sql("SELECT * FROM cim_2023_music_recording_studios").toPandas()

# Count the number of rehearsal venues per ward
df_rehearsal_count = df_rehearsal["ward_2022_name"].value_counts()

# Count the number of recording venues per ward
df_recording_studios_count = df_recording_studios["ward_2022_name"].value_counts()

# Create DataFrames with ward counts for rehearsal and recording venues
df_rehearsal_count = pd.DataFrame({'Ward': df_rehearsal_count.index, 'Rehearsal Venues': df_rehearsal_count.values})
df_recording_studios_count = pd.DataFrame({'Ward': df_recording_studios_count.index, 'Recording Venues': df_recording_studios_count.values})

# Merge the two DataFrames on the ward column
merged_df = pd.merge(df_rehearsal_count, df_recording_studios_count, on='Ward', how='outer')
merged_df = merged_df.fillna(0)

# Calculate the correlation matrix between wards for rehearsal and recording venues
correlation_matrix = merged_df.corr()

# Create a heatmap to show the correlation between wards
plt.figure(figsize=(12, 8))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", linewidths=0.5)
plt.title("Correlation Heatmap between Wards for Rehearsal and Recording Venues")
plt.show()


# MARKDOWN ********************

# ###  Copy from the Storage Data for the query - Warehouse

# CELL ********************


%%sql
--OPTIMIZE dimsalesreason_nonpart ZORDER BY (SalesReasonKey)
--OPTIMIZE factinternetsalesreason_delta ZORDER BY (SalesOrderNumber)
OPTIMIZE factinternetsales_delta ZORDER BY (SalesOrderNumber, SalesOrderLineNumber)


# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

df = spark.read.parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/sqlserverlessanalitics/DimSalesReason_NONPart/*.parquet")
df.write.mode("overwrite").format("delta").saveAsTable('DimSalesReason_NONPart')

df = spark.read.parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/sqlserverlessanalitics/FactInternetSalesReason_Delta/*.parquet")
df.write.mode("overwrite").format("delta").saveAsTable('FactInternetSalesReason_Delta')

df = spark.read.parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/administrators/sqlserverlessanalitics/FactInternetSales_Delta/*.parquet")
df.write.mode("overwrite").format("delta").saveAsTable('FactInternetSales_Delta')

# CELL ********************

