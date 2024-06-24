# Fabric notebook source


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
