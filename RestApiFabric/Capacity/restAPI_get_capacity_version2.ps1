# Define your Azure subscription, resource group, and other parameters
$grant_type="client_credentials"
$ClientId = "985134f5-4ec8-4d61-aa9d-6b0325e98ec9"
$ClientSecret = "FP98Q~z.cp0c8ukLSC1oTdIF4jF21KTxZYc5taFY"

$RequestAccessTokenUri = "https://login.microsoftonline.com/microsoft.onmicrosoft.com/oauth2/token"  # Check the Customer Token URL correctly and provide input entry

$Resource = "https://management.azure.com/"
$body = @{"grant_type"=$grant_type;"client_id"=$ClientId;"client_secret"=$ClientSecret;"resource"=$Resource}
$response = Invoke-RestMethod -Method Post -Uri $RequestAccessTokenUri -Body $body
$accessbearertoken=$response.access_token

Connect-AZAccount -AccessToken $accessbearertoken -AccountId $ClientId 



#SQL-HA-RG-New
#sql-ha-rg

# Define your Azure subscription, resource group, and other parameters
$subscriptionId = "1ca05c7d-712f-4bf6-bbae-866f523c7a7f"
$resourceGroupName = "DefaultResourceGroup-EUS"
$dedicatedCapacityName = "Trial-lilem-microsoft-com-05-18-2023-13-59-UTC"
$apiVersion = "2022-07-01-preview"


# Build the API request URL
$requestUrl = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Fabric/capacities/$dedicatedCapacityName?api-version=2022-07-01-preview"
#$requestUrl = "https://management.core.windows.net/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Fabric/capacities/$dedicatedCapacityName?api-version=2022-07-01-preview"

# Make the GET request
$response = Invoke-RestMethod -Method GET -Uri $requestUrl -Headers @{Authorization = "Bearer $((Get-AzAccessToken -ResourceUrl $resource).Token)}"}

# Display the response
$response | ConvertTo-Json

# Define your Azure subscription, resource group, and other parameters
#$subscriptionId = "1ca05c7d-712f-4bf6-bbae-866f523c7a7f"
#$dedicatedCapacityName = "Trial-lilem-microsoft-com-05-18-2023-13-59-UTC"
#$apiVersion = "2022-07-01-preview"

# List of resource group names to iterate through
#$resourceGroups = @(
#    "workspacemanagedrg-a9dac964-9442-427c-8811-8b0e51346c3b",
#    "workspacemanagedrg-66bc6a95-f2fd-4fbc-83aa-a3074b6b4b94",
#    "VMStandalone_group-asr-1",
#    "VMStandalone_group-asr",
#    "VMStandalone_group",
#    "ubuntuinsights_group_05181317",
 #   "ubuntuinsights_group",
#    "synapseworkspace-managedrg-aae71a70-2a09-4ccd-b36b-fe4ab466159e",
#    "synapseworkspace-managedrg-54f7fcd0-4392-40ac-831d-949b04b1497d",
#    "SQL-HA-RG-New",
#    "sql-ha-rg",
#    "RG_ContosoKeyVault",
#    "northeurope",
#    "NetworkWatcherRG",
#    "mifta-RG",
#    "MA_azuremonitorf_uksouth_managed",
#    "DefaultResourceGroup-WEU",
#    "DefaultResourceGroup-SUK",
#    "DefaultResourceGroup-EUS",
#    "databricks-rg-dtbrick_premium-h6cs6635l27mw",
#    "databricks-rg-Dtabrick-7tb3tnjcy2fkq",
#    "cloud-shell-storage-northeurope",
#    "ad-primary-dc_group"
#)#

# Loop through resource groups
foreach ($resourceGroupName in $resourceGroups) {
    # Build the API request URL
    $requestUrl = "https://management.core.windows.net/subscriptions/$subscriptionId/resourceGroups/$resourceGroupName/providers/Microsoft.Fabric/capacities/$dedicatedCapacityName?api-version=$apiVersion"

    # Make the GET request
    try {
        $response = Invoke-RestMethod -Method GET -Uri $requestUrl -Headers @{Authorization = "Bearer $((Get-AzAccessToken -ResourceUrl $resource).Token)}"}

        # Display the response
        $response | ConvertTo-Json
    }
    catch {
        Write-Host "Error accessing resource group: $resourceGroupName"
        Write-Host "Error message: $($_.Exception.Message)"
    }
}




