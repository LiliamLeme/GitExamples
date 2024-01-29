# Define your Azure subscription, resource group, and other parameters
$subscriptionId = "1ca05c7d-712f-4bf6-bbae-866f523c7a7f"
$resourceGroupName = "SQL-HA-RG-New"
$dedicatedCapacityName = "Trial-lilem-microsoft-com-05-18-2023-13-59-UTC"
$apiVersion = "2022-03-01-preview"

# Authenticate using the Service Principal credentials
$ClientID = '985134f5-4ec8-4d61-aa9d-6b0325e98ec9'
$ClientSecret =  'FP98Q~z.cp0c8ukLSC1oTdIF4jF21KTxZYc5taFY'
$tenantId = '72f988bf-86f1-41af-91ab-2d7cd011db47'
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