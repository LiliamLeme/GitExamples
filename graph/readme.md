## Graph example 


### MS Graph Automatic permissions.

Using python to add permissions with MS Graph

Ref:

[Grant or revoke API permissions programmatically - Microsoft Graph | Microsoft Learn](https://learn.microsoft.com/en-us/graph/permissions-grant-via-msgraph?tabs=http&pivots=grant-application-permissions)

[Authentication and authorization basics - Microsoft Graph | Microsoft Learn](https://learn.microsoft.com/en-us/graph/auth/auth-concepts#delegated-and-application-permissions)

[Assign Microsoft Entra roles to users - Microsoft Entra ID | Microsoft Learn](https://learn.microsoft.com/en-us/entra/identity/role-based-access-control/manage-roles-portal)

[List Microsoft Entra role assignments - Microsoft Entra ID | Microsoft Learn](https://learn.microsoft.com/en-us/entra/identity/role-based-access-control/view-assignments)



### Step by step

1. From Azure Portal -> App registration:

   Create the Service Principal in the App registration, take note of the Application ID and the tenant ID. This is the one that you will use for adding and removing permissions

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/1cb74a9e-3688-4af2-804a-20ac3c110deb)

   

   Also you need a **second user that could be a Service Principal** with high permissions and able to generate a token that have enough permissions to add and remove permissions. **A Global Administrator Service Principal would be able to do that.**

   

   

   2. Add a secret and save that information during the creation of the secret.

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/812d8307-1070-41de-80b7-b5b2909dd798)


Note: Keep the details of this second Service Principal with high permissions noted. You also need the Application ID and the Client Secrets from this one.

3. Open your python notebook and Install the following libraries:

   ```
   pip install msgraph-sdk
   pip install azure-identity
   pip install prettytable
   
   ```

   **More information:**

   [microsoftgraph/msgraph-sdk-python (github.com)](https://github.com/microsoftgraph/msgraph-sdk-python)

   [Azure Identity client library for Python | Microsoft Learn](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python)

   [prettytable · PyPI](https://pypi.org/project/prettytable/)

4. Once installed  get the information that you kept from step 1 and 2 and use on the following script:

   ```
   import requests
   import json
   from prettytable import PrettyTable
   
   
   async def get_service_principals():
   
       tenant_id = "REPLACE tenant ID"
       client_id = "Replace Service Principal ID"
       client_secret = "Replace Secret" 
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
   
   
   
   ```

   

From the results look for the permissions you want to add and take note of the ids, for example the following permissions under AppRoles column:

- User.Read.All
- User.ReadWrite.All   

Also Take note of the Microsoft Graph ID under the ID column:

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/c8daa9f1-1861-4e41-8b96-a807bb540ae0)
Take note of those IDs, because you will use on the next steps.

5. Open the **Enterprise Applications** in the Azure Portal, look for objectid of the application that was create at the step 1, also keep note of the same information ( objectid) for the second service principal that has very high permissions.

![image](https://github.com/LiliamLeme/GitExamples/assets/62876278/77bb0bb4-f79b-4bec-8c5c-9980e7a7d061)


6. Back to the Python Notebook.

​      6.1 Replace the information client_ID and Client_secret with the SP with high permissions. This first step will represent the credential  that must have enough permissions for the next step.

```
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from uuid import UUID
from msgraph.generated.models.app_role_assignment import AppRoleAssignment
from msgraph.generated.models.service_principal import ServicePrincipal


# THE PYTHON SDK IS IN PREVIEW.

# The client credentials flow requires that you request the
# /.default scope, and pre-configure your permissions on the
# app registration in Azure. An administrator must grant consent
# to those permissions beforehand.
scopes = ['https://graph.microsoft.com/.default']

tenant_id = "Replace with Tenant ID"
client_id = "Replace with Service Principal ID that has high permissions"
client_secret = "Replace with sercret from the SP that has high permissions" 

```

6.2 The next step you will replace with the Service Principal that you will change the permissions. For example adding those 2 permissions. You will use the Object ID as Client_ID that you got from the Enterprise Applications:

- User.Read.All

- User.ReadWrite.All   

  

In the code context:

- Resource ID is the Microsoft Graph ID that, in my example it started with 48a71369

- Client_ID is the Object ID from Enterprise Application
- App Role id is the ID of the role that you want to change

```
graph_client = GraphServiceClient(credential, scopes) # type: ignore

# Define the UUIDs
##908e6e3d-deed-4ef0-9a04-cc30bad96e7d
principal_id = UUID("Replace by - Object ID from Enterprise Application to change permission")
resource_id = UUID("Replace by -Microsft Graph ID")
app_role_id = UUID("Replace by - ID fo the permission of the role, example ID of User.Read.All")



request_body = AppRoleAssignment(
principal_id =principal_id,
resource_id = resource_id,
app_role_id = app_role_id,
)

result = await graph_client.service_principals.by_service_principal_id('Replace by Object ID from Enterprise Application ').app_role_assigned_to.post(request_body)
##service principal id from the Enterprise applications - (Object ID)

```

You will be able to see the new permission at the Enterprise Applications - PermissionsTab

Under the App registration it will be Other Permissions add at the end of the list for API permissions tab. In order to move that permission and activate you need to Grant Admin Consent.



In order to move that permission and activate you need to Grant Admin Consent.

