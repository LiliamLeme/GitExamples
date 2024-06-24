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

# CELL ********************

%pip install semantic-link
%load_ext sempy

# CELL ********************

from sempy.fabric import FabricDataFrame
from sempy.dependencies import plot_dependency_metadata
import pandas as pd


df = FabricDataFrame(pd.read_csv("/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries.csv"))

deps = df.find_dependencies()


# CELL ********************

#from sempy.fabric import FabricDataFrame
#from sempy.dependencies import plot_dependency_metadata
#from sempy.samples import download_synthea
import pandas as pd
from sempy.fabric import FabricDataFrame
from sempy.dependencies import plot_dependency_metadata
from sempy.samples import download_synthea

df = FabricDataFrame(pd.read_csv("/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries.csv"))

deps = df.find_dependencies()
plot_dependency_metadata(deps)

##https://learn.microsoft.com/en-us/fabric/data-science/semantic-link-validate-data

# CELL ********************

violations = df.list_dependency_violations(determinant_col="ward_2022_name", dependent_col="ward_2022_code")
violations

# CELL ********************

from sempy.fabric import FabricDataFrame
from sempy.dependencies import plot_dependency_metadata
from sempy.samples import download_synthea
import pandas as pd

download_synthea(which='small')

df = FabricDataFrame(pd.read_csv("/lakehouse/default/Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries.csv"))

df.plot_dependency_violations(determinant_col="ward_2022_name", dependent_col="ward_2022_code")

# CELL ********************



import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/" + "Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries.csv"
df = pd.read_csv("/lakehouse/default/" + "Files/Cultural_Infra/Cultural_Infra/Library/CIM 2023 Libraries.csv")
display(df)


# CELL ********************

df = spark.sql("SELECT name,latitude,longitude,easting,northing, ward_2022_name, ward_2022_code,borough_name FROM Airlift_LH.cim_2023_libraries where borough_name= 'Harrow'")
##--group by ward_2022_name, ward_2022_code,borough_name   having count(*)>1 ")
display(df)
##--ward_2022_name, ward_2022_code,borough_name 
##--name,latitude,longitude,easting,northing,
