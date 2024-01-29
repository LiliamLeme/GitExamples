#•	Applications.Read.All
#•	Group.Read.All
#•	User.Read.All
#•	GroupMember.Read.All

import asyncio
from azure.identity import ClientSecretCredential
from uuid import UUID
from msgraph import GraphServiceClient
from msgraph.generated.models.app_role_assignment import AppRoleAssignment
from msgraph.generated.service_principals.service_principals_request_builder import ServicePrincipalsRequestBuilder
from msgraph.generated.models.service_principal_collection_response import ServicePrincipalCollectionResponse
import pandas as pd
import json

##User with high permissions
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
client_secret = "" 
####
principal_id = "43e5b643-cd10-45de-a5be-a98619bb513d" ##Enterprise Application ID
resource_id = "48a71369-01fa-403d-8fba-cc4fb16f2062" ##Microsoft Grpah ID


###Get the crendentials to connect
def get_credential_auth(tenant_id, client_id, client_secret):
    # azure.identity.aio
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret)
    return credential


###List graph permissions
async def get_service_principals(credentials):
    # Replace these values with your actual authentication and resource details
    scopes = ['https://graph.microsoft.com/.default']

    graph_client = GraphServiceClient(credentials, scopes)

    #•	Applications.Read.All - 9a5d68dd-52b0-4cc2-bd40-abcf44ac3a30
#•	Group.Read.All - 5b567255-7703-4780-807c-7be8301ae99b
#•	User.Read.All - df021288-bdef-4463-88db-98f22de89214
#•	GroupMember.Read.All -  98830695-27a2-44f7-8c18-0c3ebc9698f6
    

    query_params = ServicePrincipalsRequestBuilder.ServicePrincipalsRequestBuilderGetQueryParameters(
        filter="displayName eq 'Microsoft Graph'",
        #filter="displayName eq 'Microsoft Graph' and appRoles/any(role: role/displayName eq 'Group.Read.All )",
        select=["id", "displayName", "appId", "appRoles"],
    )


    request_configuration = ServicePrincipalsRequestBuilder.ServicePrincipalsRequestBuilderGetRequestConfiguration(
        query_parameters=query_params,
    )

    try:
        result = await graph_client.service_principals.get(request_configuration=request_configuration)
    
        return result
    except Exception as e:
        print(f'Error: {e}')
        return None

# Example usage
async def main_list(credential):
 
    result = await get_service_principals(credential)
    print(result)

###Add permissions accordingly
async def AddPermission(credential, principal_id, resource_id, app_role_id):

    # scope
    scopes = ['https://graph.microsoft.com/.default']

    ## df021288-bdef-4463-88db-98f22de89214 (id) - User.Read.All 
    graph_client = GraphServiceClient(credential, scopes) # type: ignore

    uuid_principal_id = UUID(principal_id)
    uuid_resource_id = UUID(resource_id)
    uuid_app_role_id = UUID(app_role_id)

    request_body = AppRoleAssignment(
        principal_id=uuid_principal_id,
        resource_id=uuid_resource_id,
        app_role_id=uuid_app_role_id,
    )

    result = await graph_client.service_principals.by_service_principal_id(principal_id).app_role_assigned_to.post(request_body)
    ## service principal id from the Enterprise applications - (Object ID)
    print(result)



#######################################################################################################################################

###Flow
credential = get_credential_auth(tenant_id, client_id, client_secret)
asyncio.run(main_list(credential))

#app_role_id = "9a5d68dd-52b0-4cc2-bd40-abcf44ac3a30"
#asyncio.run(AddPermission(credential, principal_id, resource_id, app_role_id))


#•	Applications.Read.All - 9a5d68dd-52b0-4cc2-bd40-abcf44ac3a30
#•	Group.Read.All - 5b567255-7703-4780-807c-7be8301ae99b
#•	User.Read.All - df021288-bdef-4463-88db-98f22de89214
#•	GroupMember.Read.All -  98830695-27a2-44f7-8c18-0c3ebc9698f6