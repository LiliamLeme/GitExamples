import aiohttp
import json
import asyncio
import pandas as pd


# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "c52eafb7-7387-466e-ad41-98164a73256a"
client_secret = "" 

######################################################
##get the token. 
async def get_credential_token(tenant_id, client_id, client_secret):
    resource = 'https://graph.microsoft.com'
    
    # Acquire token using MSAL
    authority = f'https://login.microsoftonline.com/{tenant_id}'
    token_url = f'{authority}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': f'{resource}/.default'
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(token_url, data=token_data) as response:
            token_response = await response.json()
            access_token = token_response.get('access_token')
    return access_token

######################################################
##get the list filtered
async def get_service_principals_token(access_token, endpoint):
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

    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint, headers=headers, params=query_parameters) as response:
            if response.status == 200:
                result = await response.json()

                service_principals = result['value']
                filtered_service_principals = []
                for item in service_principals:
                    if item['displayName'] == 'Microsoft Graph' or any(role['displayName'] in ['Applications.Read.All', 'Group.Read.All', 'User.Read.All', 'GroupMember.Read.All'] for role in item.get('appRoles', [])):
                        filtered_service_principals.append(item)
                
                # Extract necessary data for DataFrame
                service_principals_data = []
                for item in filtered_service_principals:
                    service_principal_data = {
                        'id': item['id'],
                        'displayName': item['displayName'],
                        'appId': item['appId'],
                        'appRoles': item['appRoles'] if 'appRoles' in item else None
                    }
                    service_principals_data.append(service_principal_data)

                df = pd.DataFrame(service_principals_data)


                return df
            else:
                print(f'Error: {response.status}')
                print(await response.text())
                return None

######################################################
# Example usage - Flow
async def main_token(tenant_id, client_id, client_secret):
    endpoint = 'https://graph.microsoft.com/v1.0/servicePrincipals'
    access_token = await get_credential_token(tenant_id, client_id, client_secret)
    result = await get_service_principals_token(access_token, endpoint)
    print(result)

asyncio.run(main_token(tenant_id, client_id, client_secret))


