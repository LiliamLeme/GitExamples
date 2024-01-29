# Define your Azure subscription, resource group, and other parameters
$grant_type="client_credentials"
$ClientId = ""
$ClientSecret = "FP9"

$RequestAccessTokenUri = "https://login.microsoftonline.com/microsoft.onmicrosoft.com/oauth2/token"  # Check the Customer Token URL correctly and provide input entry

$Resource = "https://management.azure.com/"
$body = @{"grant_type"=$grant_type;"client_id"=$ClientId;"client_secret"=$ClientSecret;"resource"=$Resource}
$response = Invoke-RestMethod -Method Post -Uri $RequestAccessTokenUri -Body $body
$accessbearertoken=$response.access_token

Connect-AZAccount -AccessToken $accessbearertoken -AccountId $ClientId 





