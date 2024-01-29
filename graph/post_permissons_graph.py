import requests
from msal import ConfidentialClientApplication

# Replace these values with your actual authentication and resource details
tenant_id = "3c863c9b-2221-4236-88c3-37fe9e1d06f8"
client_id = "a49b035c-f602-4b26-be68-784fb7c374c9"
client_secret = "" 
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


# Make request to Microsoft Graph API to get service principals
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

url = f"{endpoint}/9490e1f6-7689-4e57-9778-fc41dafaf448/appRoleAssignments"
##

payload = {
    "principalId": "9490e1f6-7689-4e57-9778-fc41dafaf448",
    "resourceId": "48a71369-01fa-403d-8fba-cc4fb16f2062",
    "appRoleId": "d07a8cc0-3d51-4b77-b3b0-32704d1f69fa",
}

response = requests.post(url, json=payload, headers=headers)


if response.status_code == 201:
    print("POST request successful")
else:
    print(f"POST request failed with status code {response.status_code}")
    print(response.text)
