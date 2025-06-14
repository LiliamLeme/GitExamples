﻿
$subscriptionId = ""
$resourceGroupName = "SQL-HA-RG-Li"
$dedicatedCapacityName = ""
$apiVersion = "2022-07-01-preview"


# Define your Azure subscription, resource group, and other parameters
$grant_type="client_credentials"
$ClientId = ""
$ClientSecret = ""


# Construct the authentication URL
$authUrl =  "https://login.microsoftonline.com/MngEnvMCAP040685.onmicrosoft.com/oauth2/token"


# Request an access token
$token = Invoke-RestMethod -Uri $authUrl -Method Post -Body @{
    grant_type    = 'client_credentials'
    client_id     = $ClientId
    client_secret = $ClientSecret
    resource      = 'https://management.azure.com/'
}


##Write-Host "Constructed token: $ClientId"


# Construct the API request URL
 $url = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Fabric/capacities/lemefabric/resume?api-version=$apiVersion"

#Write-Host "Constructed URL: $url"

# Include the Authorization header
$headers = @{
    'Authorization' = "Bearer $($token.access_token)"
}

# Make the request
$response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers

# Output the response
$response
