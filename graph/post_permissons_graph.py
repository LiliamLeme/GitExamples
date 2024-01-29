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
#access_token = 'eyJ0eXAiOiJKV1QiLCJub25jZSI6IjNwN0t5YVdESHNNM1JyT3RNTlV3U0E5NVpLbzNHcFBMazB2dlFjZVNxRnMiLCJhbGciOiJSUzI1NiIsIng1dCI6IjVCM25SeHRRN2ppOGVORGMzRnkwNUtmOTdaRSIsImtpZCI6IjVCM25SeHRRN2ppOGVORGMzRnkwNUtmOTdaRSJ9.eyJhdWQiOiJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC8zYzg2M2M5Yi0yMjIxLTQyMzYtODhjMy0zN2ZlOWUxZDA2ZjgvIiwiaWF0IjoxNzA1NTk5MTk1LCJuYmYiOjE3MDU1OTkxOTUsImV4cCI6MTcwNTYwMzA5NSwiYWlvIjoiRTJWZ1lKajEvdlc5OTR2bFpXbyt6SnQ0STBGNE9nQT0iLCJhcHBfZGlzcGxheW5hbWUiOiJBUElfZmFicmljIiwiYXBwaWQiOiI5MDhlNmUzZC1kZWVkLTRlZjAtOWEwNC1jYzMwYmFkOTZlN2QiLCJhcHBpZGFjciI6IjEiLCJpZHAiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC8zYzg2M2M5Yi0yMjIxLTQyMzYtODhjMy0zN2ZlOWUxZDA2ZjgvIiwiaWR0eXAiOiJhcHAiLCJvaWQiOiI0NTQ3MDhmZi0zNDc5LTQwODctOTYyNS00NzkwMDY2NzQwMTQiLCJyaCI6IjAuQVh3QW16eUdQQ0VpTmtLSXd6Zi1uaDBHLUFNQUFBQUFBQUFBd0FBQUFBQUFBQUM3QUFBLiIsInJvbGVzIjpbIlVzZXIuUmVhZFdyaXRlLkFsbCIsIkFwcGxpY2F0aW9uLlJlYWRXcml0ZS5Pd25lZEJ5IiwiQXBwbGljYXRpb24uUmVhZFdyaXRlLkFsbCIsIkRpcmVjdG9yeS5SZWFkV3JpdGUuQWxsIiwiRGlyZWN0b3J5LlJlYWQuQWxsIiwiVXNlci5SZWFkLkFsbCIsIkFwcGxpY2F0aW9uLlJlYWQuQWxsIl0sInN1YiI6IjQ1NDcwOGZmLTM0NzktNDA4Ny05NjI1LTQ3OTAwNjY3NDAxNCIsInRlbmFudF9yZWdpb25fc2NvcGUiOiJOQSIsInRpZCI6IjNjODYzYzliLTIyMjEtNDIzNi04OGMzLTM3ZmU5ZTFkMDZmOCIsInV0aSI6ImdpbFdZY3BRUUVpVE1vYktrOEZTQVEiLCJ2ZXIiOiIxLjAiLCJ3aWRzIjpbIjA5OTdhMWQwLTBkMWQtNGFjYi1iNDA4LWQ1Y2E3MzEyMWU5MCJdLCJ4bXNfdGNkdCI6MTY3Njg5MTE1NH0.0V4NJ_U_1tXQJZk1rprGJT9nj1UIHKFlvtVVI5yuew_6WbrhpykZsKTnS2vVkdMjLOLfim6oukEN1HmWjCNDPAJSPXNKRSpJB3Z8qoYwnKkNpvgocl72n2VOZrROjWrswj23qxXNiPlSxm5P5goknjFjbUe_nnHXcNW4fguvKhoHcVM-Z8ngbGrvIJekiwkYD5EyZjnOgzYnggQW3hGbHSU2nvdkCaWEBGtpCENRfz8DdwyAFtY_kOonQfk7vQKyDLe7a3hKpmu8FneTi_5qUtXofn0vke2eT63_iUh7C89BLViJSRDeQCXklkTYkFoMESVKslE0IgCzUG-HXgaSOQ'
#print (access_token)

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
