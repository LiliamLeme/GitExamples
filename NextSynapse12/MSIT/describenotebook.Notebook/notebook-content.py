# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ec7e333c-a646-475e-8316-fe2a234bf38d",
# META       "default_lakehouse_name": "DataflowsStagingLakehouse",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "ec7e333c-a646-475e-8316-fe2a234bf38d"
# META         },
# META         {
# META           "id": "c79d2bcd-61e4-4e24-af61-e61d1e5a4510"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE TABLE EXTENDED nyctlc;
# MAGIC 
# MAGIC --abfss://9fee2690-4084-4127-9ba5-0ca1b1180451@msit-onelake.dfs.fabric.microsoft.com/ec7e333c-a646-475e-8316-fe2a234bf38d/Tables/nyctlc
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************


import requests
import time
from urllib.parse import urlparse



# Include the Authorization header
####f12-> Console -> copy(powerBIAccessToken)
headers = {
    "Authorization": f"Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlhSdmtvOFA3QTNVYVdTblU3Yk05blQwTWpoQSIsImtpZCI6IlhSdmtvOFA3QTNVYVdTblU3Yk05blQwTWpoQSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTcxMDI0Nzk1MywibmJmIjoxNzEwMjQ3OTUzLCJleHAiOjE3MTAyNTM1MjUsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBYlFBUy84V0FBQUE3b1lMYU4ySGMzNG1VaFRjb1hKaVFLVjN6QVVna3lzZ2JKL1NjMlJBT1YxM1o5dHUxS1h5V3E3djJGRVByY0Rpb1hqOGxha1JYMTNWYVpsSG5SNVk1UU9lcTJhV0Yxd012OFI2SVg2T1gzRnBRTHZVeEFLOEh5ZnFya3NkdnAzc2tIT2d6REVadVl3MzRxZHRwZWVGczl4bDFhODJ2UXBreWMzbDZRVEs5Y0hDdVdLYStpN0lVV3B0dlRvbFRtME92d2tidVlLNE9kdzZCamRKb21kTUt6ZDI2Q2M4aDhZaUdINTJPWVlDdDkwPSIsImFtciI6WyJyc2EiLCJtZmEiXSwiYXBwaWQiOiI4NzFjMDEwZi01ZTYxLTRmYjEtODNhYy05ODYxMGE3ZTkxMTAiLCJhcHBpZGFjciI6IjAiLCJjb250cm9scyI6WyJhcHBfcmVzIl0sImNvbnRyb2xzX2F1ZHMiOlsiODcxYzAxMGYtNWU2MS00ZmIxLTgzYWMtOTg2MTBhN2U5MTEwIiwiMDAwMDAwMDktMDAwMC0wMDAwLWMwMDAtMDAwMDAwMDAwMDAwIiwiMDAwMDAwMDMtMDAwMC0wZmYxLWNlMDAtMDAwMDAwMDAwMDAwIl0sImRldmljZWlkIjoiMjY3NzhiZDAtMzUyZi00Y2U2LThiMWEtNDEzODNjZmFiZWFmIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiJlNmU3MTAwNS1kMGZiLTRmZmUtODcxMC1lMzUyYzM2MmZkMTYiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMTcyMTI1NDc2My00NjI2OTU4MDYtMTUzODg4MjI4MS0zODc2MDQ4IiwicHVpZCI6IjEwMDM3RkZFODdFMUNERkQiLCJyaCI6IjAuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUndrQUFBQUFBQUFBd0FBQUFBQUFBQUFhQUZJLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInNpZ25pbl9zdGF0ZSI6WyJkdmNfbW5nZCIsImR2Y19jbXAiLCJrbXNpIl0sInN1YiI6IjhYQW5YLVBaTUlMcjM3bUdyNmRlVi1MSVA0bTE4d3ZEUVRYVlVjQ3lMNVEiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6ImxpbGVtQG1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxlbUBtaWNyb3NvZnQuY29tIiwidXRpIjoiY2diU3dDeVdGRVNBd1l0dVRna1lBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19jYyI6WyJDUDEiXX0.n9kXLlQrGTJp7UBUzcPJ4QCxWLO4CmglaCi-haIxi_XrSFev2XIjh8lm0f2q5LnqzmfCMRgw2-hmF5NGPFemvsbedEKcTehxqBzJioNqiGcPz5qJ8OIHE5eHmUV55adLjpbd89zZVJ9a1NOUw8pomolj8FJQkEGXekfAejkgTK8bz8apw_0Ega-SF8Pnb1bJt6PcK9fOQ4uIEA678EpUP1uxrvA72qghwcubZrNqo0OPFOOZz3vtTTwCtO6qb3bmG9gQx5d_35wDCx01UxMQRWehz146zvOC9xIhTm_KSFZRsf8PKKbkMd7ADAfCjBXHTMtpri3TQk4p7q3ON9oMDA",
}

# METADATA ********************

# META {}

# CELL ********************

url_get= f"https://api.fabric.microsoft.com/v1/workspaces/9fee2690-4084-4127-9ba5-0ca1b1180451/lakehouses/ec7e333c-a646-475e-8316-fe2a234bf38d"
response_status = requests.get(url_get, headers=headers)
response_data = response_status.json()
status = response_data.get('status')

print (response_data)


#abfss://3d631752-8cfd-48dd-a1e4-192d37311254@msit-onelake.dfs.fabric.microsoft.com/c79d2bcd-61e4-4e24-af61-e61d1e5a4510/Tables/nyctlc
