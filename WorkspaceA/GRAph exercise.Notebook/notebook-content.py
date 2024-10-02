# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c557e0a3-13eb-43df-8750-bfbf032e663c",
# META       "default_lakehouse_name": "LKA",
# META       "default_lakehouse_workspace_id": "d21dfb2b-a460-4822-8724-39cef0dc18f2",
# META       "known_lakehouses": [
# META         {
# META           "id": "c557e0a3-13eb-43df-8750-bfbf032e663c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### SDK

# MARKDOWN ********************

# ### Install the following

# CELL ********************

%pip install msgraph-beta-sdk


# CELL ********************


%pip install azure-identity

# CELL ********************

##install SDK for msgrpah

%pip install msgraph-sdk --upgrade pip
##https://learn.microsoft.com/en-us/graph/sdks/sdk-installation
##https://pypi.org/project/msgraph-sdk/

# MARKDOWN ********************

# ### Get the necessary configuration. Tenant id, client, secret from App Registrations in the portal

# MARKDOWN ********************

# ### Listing permissions per SP
# - Replace the SP ID from App Registration ( Azure Portal)
# - Replace the tenant id and Secret.

# CELL ********************

import requests
import json

##it needs thje application. Read user all. Otherwise fails
##https://learn.microsoft.com/en-us/graph/permissions-grant-via-msgraph?tabs=csharp&pivots=grant-application-permissions#code-try-0
async def get_service_principals():
    # Replace these values with your actual authentication and resource details
    tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
    client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
    client_secret = "m0L8Q~dy6n6X62_K90XNIDSPWS3_lp02fVRbgc51" 
    resource = 'https://graph.microsoft.com'
    endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'

    # Acquire token using MSAL
    authority = f'https://login.microsoftonline.com/{tenant_id}'
    token_url = f'{authority}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': f'{resource}/.default'
    }

    token_response = requests.post(token_url, data=token_data)
    access_token = token_response.json().get('access_token')

    # Make request to Microsoft Graph API
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    query_parameters = {
        '$filter': "displayName eq 'Microsoft Graph'",
        '$select': 'id,displayName,appId,appRoles'
    }

    response = requests.get(endpoint, headers=headers, params=query_parameters)

    # Process the response
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None

# Example usage
result = await get_service_principals()
print(result)

############################################################
###important result this is the resourceID that will be used on the other CELLS:
###{'@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#servicePrincipals(id,displayName,appId,appRoles)', 'value': [{'id': '48a71369-01fa-403d-8fba-cc4fb16f2062', 'displayName': 'Microsoft Graph', 'appId': '00000003-0000-0000-c000-000000000000', 

# MARKDOWN ********************

# ### Install Pretty table to order the last results in a readable format

# CELL ********************

pip install prettytable


# MARKDOWN ********************

# ### Listing with a better format
# - Use the same information replaced from the script above and get the results in a better format
# - From here copy and past the ID of Microsoft Graph and the ID of the permissions to be given to the SP


# CELL ********************

import requests
import json
from prettytable import PrettyTable

##it needs thje application. Read user all. Otherwise fails
##https://learn.microsoft.com/en-us/graph/permissions-grant-via-msgraph?tabs=csharp&pivots=grant-application-permissions#code-try-0
async def get_service_principals():
    # Replace these values with your actual authentication and resource details
    tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
    client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
    client_secret = "ydX8Q~D~aWqme6_QxfrJGaNA1OrZ5k5Lb7kY5cyb" 
    resource = 'https://graph.microsoft.com'
    endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'

    # Acquire token using MSAL
    authority = f'https://login.microsoftonline.com/{tenant_id}'
    token_url = f'{authority}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': f'{resource}/.default'
    }

    token_response = requests.post(token_url, data=token_data)
    access_token = token_response.json().get('access_token')

    # Make request to Microsoft Graph API
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    query_parameters = {
        '$filter': "displayName eq 'Microsoft Graph'",
        '$select': 'id,displayName,appId,appRoles'
    }

    response = requests.get(endpoint, headers=headers, params=query_parameters)

    # Process the response
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        print(f'Error: {response.status_code}')
        print(response.text)
        return None

result = await get_service_principals()


if result:
    # Create a PrettyTable instance with explicit column widths
    table = PrettyTable()
    table.field_names = ['ID', 'DisplayName', 'AppID', 'AppRoles']
    
    # Set column widths
    table.max_width['ID'] = 36
    table.max_width['DisplayName'] = 20
    table.max_width['AppID'] = 40
    table.max_width['AppRoles'] = 120

    
    # Extract relevant information from the result
    for principal in result.get('value', []):
        principal_id = principal.get('id', '')
        display_name = principal.get('displayName', '')
        app_id = principal.get('appId', '')
        
        # Extract appRoles and concatenate id and value for display
        app_roles_list = principal.get('appRoles', [])
        app_roles = ', '.join([f"{role['id']} (id) - {role['value']} \n"  for role in app_roles_list])

        # Add a row to the table
        table.add_row([principal_id, display_name, app_id, app_roles])

# Print the table
print(table)




# MARKDOWN ********************

# #### Get current configuration details
# - This will list the same configuration of object ID that can be obtained from the Enterprise Application (Azure POrtal)
# - You will need the Service Principal information for the next Script

# CELL ********************

tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
client_secret = "ydX8Q~D~aWqme6_QxfrJGaNA1OrZ5k5Lb7kY5cyb" 
resource_url = 'https://graph.microsoft.com'
service_principal_display_name = "APP_Fabric"

# Get access token using client credentials flow
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
token_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': f'{resource_url}/.default'
}

###https://login.microsoftonline.com/common/adminconsent?client_id=6731de76-14a6-49ae-97bc-6eba6914391e&state=12345&redirect_uri=https://localhost/myapp/permissions

###https://graph.microsoft.com/.default

token_response = requests.post(token_url, data=token_data)
access_token = token_response.json().get('access_token')

# Get service principal details
sp_query_url = f"{resource_url}/v1.0/servicePrincipals"
sp_params = {
    '$filter': f'displayName eq \'{service_principal_display_name}\'',
    '$select': 'id,displayName,appId,appRoles'
}

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

sp_response = requests.get(sp_query_url, params=sp_params, headers=headers)
sp_data = sp_response.json()

if 'value' in sp_data and len(sp_data['value']) > 0:
    service_principal = sp_data['value'][0]
    print(f"Service Principal ID: {service_principal['id']}")
    print(f"Service Principal App ID: {service_principal['appId']}")
    print(f"Service Principal Display Name: {service_principal['displayName']}")
    print(f"Service Principal App Roles: {service_principal.get('appRoles', [])}")
else:
    print("Service principal not found.")


# MARKDOWN ********************

# ### Listing (all) the service principals

# CELL ********************

import requests
from msal import ConfidentialClientApplication

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "908e6e3d-deed-4ef0-9a04-cc30bad96e7d"
client_secret = "kHz8Q~UlROmkvI-ZqTdCwg6yssGVhPf~uCnlpcvj" 
resource = 'https://graph.microsoft.com'
endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'

# Acquire token using MSAL
authority = f'https://login.microsoftonline.com/{tenant_id}'
app = ConfidentialClientApplication(
    client_id,
    authority=authority,
    client_credential=client_secret,
)

token_response = app.acquire_token_for_client(scopes=[f'{resource}/.default'])
access_token = token_response['access_token']

# Make request to Microsoft Graph API to get service principals
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

response = requests.get(endpoint, headers=headers)

# Process the response
if response.status_code == 200:
    service_principals = response.json().get('value', [])
    for principal in service_principals:
        print(f"Service Principal ID: {principal.get('id')}")
        print(f"Service Principal Display Name: {principal.get('displayName')}")
        print("\n")
else:
    print(f'Error: {response.status_code}')
    print(response.text)


# MARKDOWN ********************

# ### Permission
# - Resource ID is the Microsoft Graph ID that was obtained from the script that list all graph permissions
# - Client_ID is the Object ID from Enterprise Application, the same that is on the list for Service Principal in Getting the current configuration Script
# - App Role id is the ID of the role that you want to change, he id of the role is under App role for the script that list all graph permissions.
# - Credentials must use a SP that has global administrator permissions.

# CELL ********************

from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from uuid import UUID
#from msgraph.generated.models import app_role_assignment 
from msgraph.generated.models.app_role_assignment import AppRoleAssignment
from msgraph.generated.models.service_principal import ServicePrincipal



##https://learn.microsoft.com/en-us/graph/tutorials/python?tabs=aad&tutorial-step=3
##https://github.com/microsoftgraph/msgraph-sdk-python/blob/c1cf802f3ca7f6dc4c22cc9eea765e72c7f9721a/msgraph/generated/models/app_role_assignment.py#L4

# THE PYTHON SDK IS IN PREVIEW. FOR NON-PRODUCTION USE ONLY

# The client credentials flow requires that you request the
# /.default scope, and pre-configure your permissions on the
# app registration in Azure. An administrator must grant consent
# to those permissions beforehand.
scopes = ['https://graph.microsoft.com/.default']


tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
client_secret = "ydX8Q~D~aWqme6_QxfrJGaNA1OrZ5k5Lb7kY5cyb" 


#tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
#client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
#client_secret = "m0L8Q~dy6n6X62_K90XNIDSPWS3_lp02fVRbgc51" 

# azure.identity.aio
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret)


## df021288-bdef-4463-88db-98f22de89214 (id) - User.Read.All 
graph_client = GraphServiceClient(credential, scopes) # type: ignore


    
# Define the UUIDs
##908e6e3d-deed-4ef0-9a04-cc30bad96e7d
#principal_id = UUID("9490e1f6-7689-4e57-9778-fc41dafaf448")
#resource_id = UUID("48a71369-01fa-403d-8fba-cc4fb16f2062")
#app_role_id = UUID("21792b6c-c986-4ffc-85de-df9da54b52fa")


principal_id = UUID("43e5b643-cd10-45de-a5be-a98619bb513d")
resource_id = UUID("48a71369-01fa-403d-8fba-cc4fb16f2062")
app_role_id = UUID("df021288-bdef-4463-88db-98f22de89214")


request_body = AppRoleAssignment(
principal_id =principal_id,
resource_id = resource_id,
app_role_id = app_role_id,
)

result = await graph_client.service_principals.by_service_principal_id('43e5b643-cd10-45de-a5be-a98619bb513d').app_role_assigned_to.post(request_body)##service principal id from the Enterprise applications - (Object ID)


##result = await graph_client.service_principals.by_service_principal_id('servicePrincipal-id').app_role_assigned_to.post(request_body)
##App_Fabric', principal_id=UUID('43e5b643-cd10-45de-a5be-a98619bb513d'), 
##principal_type='ServicePrincipal', resource_display_name='Microsoft Graph', 
##resource_id=UUID('48a71369-01fa-403d-8fba-cc4fb16f2062')),
##AppRoleAssignment(additional_data={}, id='kmqXR98_fkCZzD4C0HQ98gGmPoMgrE5JqNmfShekrWI', 
##odata_type='#microsoft.graph.appRoleAssignment', deleted_date_time=None, app_role_id=UUID('df021288-bdef-4463-88db-98f22de89214'), created_date_time=DateTime(2023, 11, 15, 21, 28, 33, 456588, tzinfo=Timezone('UTC'))


###https://learn.microsoft.com/en-us/graph/sdks/choose-authentication-providers?tabs=python
##https://github.com/microsoftgraph/msgraph-training-python
##https://github.com/search?q=repo%3Amicrosoftgraph%2Fmsgraph-beta-sdk-dotnet%20AppRoleAssignment&type=code
##https://github.com/microsoftgraph/msgraph-sdk-python/blob/c1cf802f3ca7f6dc4c22cc9eea765e72c7f9721a/msgraph/generated/models/app_role_assignment.py#L16
##https://stackoverflow.com/questions/45203126/how-to-use-approleassignment-in-graph-api


# MARKDOWN ********************

# ### Add permission with post
# - Same as above but using post to add the permissions instead of the SDK.
# - Token generation must use a SP that has global administrator permissions.

# CELL ********************

#GraphServiceClient _graphServiceClient = new GraphServiceClient();
#var currentUser = await _graphServiceClient.Me.Request().GetAsync();

import asyncio

from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient

# Create a credential object. Used to authenticate requests
credential = DeviceCodeCredential(
    client_id='CLIENT_ID',
    tenant_id='TENANT_ID',
    )

scopes = ["User.Read"]

# Create an API client with the credentials and scopes.
client = GraphServiceClient(credentials=credential, scopes=scopes)

# GET A USER USING THE USER ID (GET /users/{id})
async def get_user():
    user = await client.users_by_id('USER_ID').get()
    if user:
        print(user.user_principal_name, user.display_name, user.id)
asyncio.run(get_user())

# CELL ********************

import requests
from msal import ConfidentialClientApplication

# Replace these values with your actual authentication and resource details
#tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
#client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
#client_secret = "ydX8Q~D~aWqme6_QxfrJGaNA1OrZ5k5Lb7kY5cyb" 

tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
client_secret = "m0L8Q~dy6n6X62_K90XNIDSPWS3_lp02fVRbgc51" 

resource = 'https://graph.microsoft.com'
endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'

# Acquire token using MSAL
authority = f'https://login.microsoftonline.com/{tenant_id}'
app = ConfidentialClientApplication(
    client_id = client_id,
    authority=authority,
    client_credential=client_secret
)

token_response = app.acquire_token_for_client(scopes=[f'{resource}/.default'])
access_token = token_response['access_token']
#access_token = 'eyJ0eXAiOiJKV1QiLCJub25jZSI6IlVKQkkzX0taMjA5b0U4MzRIM0NaNTFBVHIwX2JpOE41S2d5emVRNGtkc28iLCJhbGciOiJSUzI1NiIsIng1dCI6IjVCM25SeHRRN2ppOGVORGMzRnkwNUtmOTdaRSIsImtpZCI6IjVCM25SeHRRN2ppOGVORGMzRnkwNUtmOTdaRSJ9.eyJhdWQiOiJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20vIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvM2M4NjNjOWItMjIyMS00MjM2LTg4YzMtMzdmZTllMWQwNmY4LyIsImlhdCI6MTcwNTU3NDM2MywibmJmIjoxNzA1NTc0MzYzLCJleHAiOjE3MDU2NjEwNjQsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBVFFBeS84VkFBQUFZQjN2OUdrS0s1MjJya3g2V01yNXdHaURtWGd5NU03OElvMHU0bWx0ZkYwVis5MWJJLzErR2xSbXF6ZHYwOXJlIiwiYW1yIjpbInB3ZCJdLCJhcHBfZGlzcGxheW5hbWUiOiJNaWNyb3NvZnRfQUFEX1JlZ2lzdGVyZWRBcHBzIiwiYXBwaWQiOiIxOGVkMzUwNy1hNDc1LTRjY2ItYjY2OS1kNjZiYzlmMmEzNmUiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6IkxlbWUiLCJnaXZlbl9uYW1lIjoiTGlsaWFtIiwiaWR0eXAiOiJ1c2VyIiwiaXBhZGRyIjoiODEuMTA2LjIwMC43NSIsIm5hbWUiOiJMaWxpYW0gTGVtZSIsIm9pZCI6IjYzZDU5MGI5LTIzNDAtNGVjOC05Nzk4LTIxNTUzNDNiMmQwMCIsInBsYXRmIjoiMyIsInB1aWQiOiIxMDAzMjAwMzAzNTQyMEY0IiwicmgiOiIwLkFYd0FtenlHUENFaU5rS0l3emYtbmgwRy1BTUFBQUFBQUFBQXdBQUFBQUFBQUFDN0FEcy4iLCJzY3AiOiJBZG1pbmlzdHJhdGl2ZVVuaXQuUmVhZC5BbGwgQWRtaW5pc3RyYXRpdmVVbml0LlJlYWRXcml0ZS5BbGwgQXBwbGljYXRpb24uUmVhZC5BbGwgQXBwbGljYXRpb24uUmVhZFdyaXRlLkFsbCBBcHBSb2xlQXNzaWdubWVudC5SZWFkV3JpdGUuQWxsIERlbGVnYXRlZFBlcm1pc3Npb25HcmFudC5SZWFkV3JpdGUuQWxsIERvbWFpbi5SZWFkLkFsbCBlbWFpbCBJZGVudGl0eVByb3ZpZGVyLlJlYWRXcml0ZS5BbGwgb3BlbmlkIE9yZ2FuaXphdGlvbi5SZWFkV3JpdGUuQWxsIFBvbGljeS5SZWFkLkFsbCBQb2xpY3kuUmVhZFdyaXRlLkFwcGxpY2F0aW9uQ29uZmlndXJhdGlvbiBQb2xpY3kuUmVhZFdyaXRlLk1vYmlsaXR5TWFuYWdlbWVudCBwcm9maWxlIFJvbGVNYW5hZ2VtZW50LlJlYWRXcml0ZS5FeGNoYW5nZSBVc2VyLlJlYWQgVXNlci5SZWFkLkFsbCIsInN1YiI6IlpDQUdZaVlsUFJSNnRCZkVEeUZGZWJwR0dRYkRUbDZSVXpkbHJ4ZE9HXzAiLCJ0ZW5hbnRfcmVnaW9uX3Njb3BlIjoiTkEiLCJ0aWQiOiIzYzg2M2M5Yi0yMjIxLTQyMzYtODhjMy0zN2ZlOWUxZDA2ZjgiLCJ1bmlxdWVfbmFtZSI6ImxpbGlhbS5sZW1lQE1uZ0Vudk1DQVAwNDA2ODUub25taWNyb3NvZnQuY29tIiwidXBuIjoibGlsaWFtLmxlbWVATW5nRW52TUNBUDA0MDY4NS5vbm1pY3Jvc29mdC5jb20iLCJ1dGkiOiJRZWsxTGRfbURVeTlhWHgybm5RMkFRIiwidmVyIjoiMS4wIiwid2lkcyI6WyI2MmU5MDM5NC02OWY1LTQyMzctOTE5MC0wMTIxNzcxNDVlMTAiLCJhOWVhODk5Ni0xMjJmLTRjNzQtOTUyMC04ZWRjZDE5MjgyNmMiLCJmZTkzMGJlNy01ZTYyLTQ3ZGItOTFhZi05OGMzYTQ5YTM4YjEiLCJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX2NjIjpbImNwMSJdLCJ4bXNfc3NtIjoiMSIsInhtc19zdCI6eyJzdWIiOiI2ZG5QWHZEZHVBQ1hWMGhoWm5tRUpxRVpNbHB4blpiZXBIWHlLdFAtRmxVIn0sInhtc190Y2R0IjoxNjc2ODkxMTU0fQ.OLfcv-mgB9aC-a25We6L21LV96k_R3lquYHLvzFKsQXkASyAxOfZRIc9Nadq1nc9a1Q2TbwM7V60DJJwy3WZTwhb3LKauxA_dlzAWpdap444vfDnfznVLD52iPdQ7KcfzOg57cH2Fco0-cTBCOVvpyIxVSAPHkF7HdAuZTjvBtxMiDreSXb9QEpK5zNbfm-L51Dqh4sB8g7ypIiw-WkLPZX7PiNpclGrK2K9B8eJ2ewjq_EGh4R3oLWtYVLv2LRwIZ5l1kphLuBv7837aYfvHW_oCcEYApSLvhIir_4brEEmiDDV6GH1biaSsjb26HmMZw-w27GRZb5UzcsfhZK7OA'
#print (access_token)

# Make request to Microsoft Graph API to get service principals
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

#url = f"{endpoint}/48a71369-01fa-403d-8fba-cc4fb16f2062/appRoleAssignments"

url = f"{endpoint}/43e5b643-cd10-45de-a5be-a98619bb513d/appRoleAssignments"
##





payload = {
    "principalId": "43e5b643-cd10-45de-a5be-a98619bb513d",
    #"principalId": "9490e1f6-7689-4e57-9778-fc41dafaf448 ",
    "resourceId": "48a71369-01fa-403d-8fba-cc4fb16f2062",
    "appRoleId": "df021288-bdef-4463-88db-98f22de89214",
}
##741f803b-c850-494e-b5df-cde7c675a1ca usert read write

response = requests.post(url, json=payload, headers=headers)


if response.status_code == 201:
    print("POST request successful")
else:
    print(f"POST request failed with status code {response.status_code}")
    print(response.text)


# MARKDOWN ********************

# ### Other tests

# CELL ********************

import requests

url = "https://login.microsoftonline.com/{tenant}/adminconsent"
state = "12345"
redirect_uri = "https://localhost"

params = {
    "client_id": client_id,
    "state": state,
    "redirect_uri": redirect_uri,
}

response = requests.get(url, params=params)

if response.status_code == 200:
    print("GET request successful")
    print(response.text)
else:
    print(f"GET request failed with status code {response.status_code}")
    print(response.text)


# MARKDOWN ********************

# ### Test with authentication using the device

# CELL ********************

from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient
###https://learn.microsoft.com/en-us/graph/sdks/create-client?tabs=python
###https://developer.microsoft.com/en-us/graph/gallery/?filterBy=Samples
###https://github.com/microsoftgraph/msgraph-sdk-design
##https://learn.microsoft.com/en-us/graph/sdks/create-client?from=snippets&tabs=csharp

##https://learn.microsoft.com/en-us/graph/tutorials/python?tabs=aad&tutorial-step=3

tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
client_secret = "ydX8Q~D~aWqme6_QxfrJGaNA1OrZ5k5Lb7kY5cyb" 


scopes = ['User.Read']

# Multi-tenant apps can use "common",
# single-tenant apps must use the tenant ID from the Azure portal



# azure.identity
credential = DeviceCodeCredential(
    tenant_id=tenant_id,
    client_id=client_id)

graph_client = GraphServiceClient(credential, scopes)


# GET https://graph.microsoft.com/v1.0/me
user = await graph_client.me.get()


# CELL ********************

from configparser import SectionProxy
from azure.identity import DeviceCodeCredential
from msgraph import GraphServiceClient
from msgraph.generated.users.item.user_item_request_builder import UserItemRequestBuilder
from msgraph.generated.users.item.mail_folders.item.messages.messages_request_builder import (
    MessagesRequestBuilder)
from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
    SendMailPostRequestBody)
from msgraph.generated.models.message import Message
from msgraph.generated.models.item_body import ItemBody
from msgraph.generated.models.body_type import BodyType
from msgraph.generated.models.recipient import Recipient
from msgraph.generated.models.email_address import EmailAddress

# MARKDOWN ********************

# ### Listing SP information from Enterprise

# CELL ********************

# THE PYTHON SDK IS IN PREVIEW. FOR NON-PRODUCTION USE ONLY

graph_client = GraphServiceClient(credential, scopes)


result = await graph_client.service_principals.by_service_principal_id("43e5b643-cd10-45de-a5be-a98619bb513d").app_role_assigned_to.get()


print (result)
