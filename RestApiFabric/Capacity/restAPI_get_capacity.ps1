# Define your Azure subscription, resource group, and other parameters
$subscriptionId = ""
$resourceGroupName = "SQL-HA-RG-New"
$dedicatedCapacityName = ""
$apiVersion = "2022-03-01-preview"

# Authenticate using the Service Principal credentials
$ClientID = ''
$ClientSecret =  ''
$tenantId = '
$resource = 'https://management.azure.com/'
$ServicePrincipalName = "app_fabric"

# Sign in to Azure using the Service Principal
Connect-AzAccount -ServicePrincipal -Tenant $tenantId -ApplicationId $ClientID -CertificateThumbprint $ClientSecret 


# Build the API request URL
$requestUrl = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Fabric/capacities/$dedicatedCapacityName?api-version=$apiVersion"

# Make the GET request
$response = Invoke-RestMethod -Method GET -Uri $requestUrl -Headers @{Authorization = "Bearer $((Get-AzAccessToken -ResourceUrl $resource).Token)}"}

# Display the response
$response | ConvertTo-Json
