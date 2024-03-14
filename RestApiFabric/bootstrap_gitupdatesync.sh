#!/bin/bash

workspaceName="leme3"        # The name of the workspace

shouldDisconnect=false                 # Determines whether the workspace should be disconnected before connecting to a new Azure DevOps connection details.

# AzureDevOps details
azureDevOpsDetails='{
    "gitProviderType": "AzureDevOps",
    "organizationName": "lileme",
    "projectName": "testcicd",
    "repositoryName": "testcicd",
    "branchName": "main",
    "directoryName": "/main"
}'

# End Parameters =======================================

baseUrl="https://api.fabric.microsoft.com/v1" # Replace with environment-specific base URL. For example: "https://api.fabric.microsoft.com/v1"

resourceUrl="https://api.fabric.microsoft.com"

declare -A fabricHeaders

# Login to Azure
az account clear
az login 
az account set --subscription "1ca05c7d-712f-4bf6-bbae-866f523c7a7f"

# Get authentication
fabricToken="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IlhSdmtvOFA3QTNVYVdTblU3Yk05blQwTWpoQSIsImtpZCI6IlhSdmtvOFA3QTNVYVdTblU3Yk05blQwTWpoQSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTcxMDMzNTQ0MywibmJmIjoxNzEwMzM1NDQzLCJleHAiOjE3MTAzMzk0NjEsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBYlFBUy84V0FBQUFOdHlDejdqbmxHTm9kU1A4bnBpVDNDN25UTkhXNVRVTnhXWElVNFY4d20zWXIxT3dxcmdtaUd6ZHRnL0NwS3JndW5KLzNUTHY0VnJPN0ppSTExUnFNZy83VXdlOFRDV256ZVE0N3JQVkZ6cExPRTI3eWdIZGlnZ0k1MGtYWEM5Y1dGbHFSVGNuaUwwOHZXZnVKQTRENm5uUmFtVG4zeGpUY2s2ZVdIaTNDMzRqTGNmRmdnVHVCWGNDOUIxZGtFVDJwcTBNM3Z5L3dla3c2WW4xakxwbzIrbUpacHQwWjlldXZtV2crU3V4akRVPSIsImFtciI6WyJyc2EiLCJtZmEiXSwiYXBwaWQiOiI4NzFjMDEwZi01ZTYxLTRmYjEtODNhYy05ODYxMGE3ZTkxMTAiLCJhcHBpZGFjciI6IjAiLCJjb250cm9scyI6WyJhcHBfcmVzIl0sImNvbnRyb2xzX2F1ZHMiOlsiMDAwMDAwMDktMDAwMC0wMDAwLWMwMDAtMDAwMDAwMDAwMDAwIiwiMDAwMDAwMDMtMDAwMC0wZmYxLWNlMDAtMDAwMDAwMDAwMDAwIl0sImRldmljZWlkIjoiMjY3NzhiZDAtMzUyZi00Y2U2LThiMWEtNDEzODNjZmFiZWFmIiwiZmFtaWx5X25hbWUiOiJMZW1lIiwiZ2l2ZW5fbmFtZSI6IkxpbGlhbSIsImlwYWRkciI6IjgxLjEwNi4yMDAuNzUiLCJuYW1lIjoiTGlsaWFtIExlbWUiLCJvaWQiOiJlNmU3MTAwNS1kMGZiLTRmZmUtODcxMC1lMzUyYzM2MmZkMTYiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMTcyMTI1NDc2My00NjI2OTU4MDYtMTUzODg4MjI4MS0zODc2MDQ4IiwicHVpZCI6IjEwMDM3RkZFODdFMUNERkQiLCJyaCI6IjAuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUndrQUFBQUFBQUFBd0FBQUFBQUFBQUFhQUZJLiIsInNjcCI6InVzZXJfaW1wZXJzb25hdGlvbiIsInNpZ25pbl9zdGF0ZSI6WyJkdmNfbW5nZCIsImR2Y19jbXAiLCJrbXNpIl0sInN1YiI6IjhYQW5YLVBaTUlMcjM3bUdyNmRlVi1MSVA0bTE4d3ZEUVRYVlVjQ3lMNVEiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6ImxpbGVtQG1pY3Jvc29mdC5jb20iLCJ1cG4iOiJsaWxlbUBtaWNyb3NvZnQuY29tIiwidXRpIjoiSmhPcC10ZnU0MHkzUXdYUDl5NG5BQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19jYyI6WyJDUDEiXX0.fmq78Flsd7KxqYTyYu-EY292PAsvcXfsxNDxo2rk_6jr8BcZbbtm0XQcY7XE3PLQkh1ADwuU5xtoOcQuV0-N0-38cRp9ipoDLIeLQ1B63_Wo78M08WibVcyDiKdVXBxEQESyaY7mv7c_os-KGZYD2R0cZB6YgulfN63ZURWhp8sdtR27YfsCx02zO_gEpB90O4LS9zsV5HryyZz6h0ZpeMJp5Pi0DuTAdXVpOKdBeCLdpvB5k8AlFpZuKNKO-b5SJzf61nH-PHVS7w5EC_oz9mF63qS3czkIt2zOUbUoF5RuphyIBSyd540RiQBjcJfmMG2PosghG6xI4w4ZnhbBCQ"

# Set fabric headers
fabricHeaders=$(cat <<EOF
{
    "Content-Type": "application/json",
    "Authorization": "Bearer $fabricToken"
}
EOF
)
getWorkspaceByName() {
    # Get workspaces    
   getWorkspacesUrl="$baseUrl/workspaces"
   workspaces_response=$(curl -s -H "Content-Type: application/json" -H "Authorization: Bearer $fabricToken" "$getWorkspacesUrl")

   #echo "Workspaces response: $workspaces_response"

   # Extract workspace name from the response
   workspace=$(echo "$workspaces_response" | grep -o '{"id":"[^"]*","displayName":"'"$workspaceName"'"[^}]*}')

   #Id=$(echo "$workspace" | grep -o '"[^"]*"')


    #workspaces=$(echo "$workspaces_response" | jq '.')

    #echo $workspaces_response
    echo $workspace

    #echo "$Id"
}

{
    workspace=$(getWorkspaceByName "$workspaceName")
    echo "Workspace information: $workspace"
    
    json=$workspace
    Id=$(echo "$json" | grep -o '"id":"[^"]*"'| awk -F '"' '{print $4}')
    echo "Id: $Id"

    # Verify the existence of the requested workspace
    if [ -z "$workspace" ]; then
        echo "A workspace with the requested name was not found." >&2
        exit 1
    fi
    
    if [ "$shouldDisconnect" = true ]; then
        # Disconnect from Git
        echo "Disconnecting the workspace '$workspaceName' from Git."

        disconnectUrl="$baseUrl/workspaces/$Id/git/disconnect"
        curl -s -X POST -H "${fabricHeaders[@]}" "$disconnectUrl"

        echo "The workspace '$workspaceName' has been successfully disconnected from Git."
    fi

    # Connect to Git
    echo "Connecting the workspace '$workspaceName' to Git."

    connectUrl="$baseUrl/workspaces/$Id/git/connect"
    
    connectToGitBody='{
        "gitProviderDetails": '"$azureDevOpsDetails"'
    }'

    #connect=$(curl -s -X POST -H "${fabricHeaders[@]}" -d "$connectToGitBody" "$connectUrl")
    connect=$(curl -X POST -H "Content-Type: application/json" -H "Authorization: Bearer $fabricToken" -d "$connectToGitBody" "$connectUrl")

    echo "The workspace: $connect"
    #echo "The workspace '$workspaceName' has been successfully connected to Git."

    # Initialize Connection
    echo "Initializing Git connection for workspace '$workspaceName'."

    initializeConnectionUrl="$baseUrl/workspaces/$Id/git/initializeConnection"
    #initializeConnectionResponse=$(curl -s -X POST -H "${fabricHeaders[@]}" -d '{}' "$initializeConnectionUrl")

    # Send the POST request
    initializeConnectionResponse=$(curl -X POST -H "Content-Type: application/json" -H "Authorization: Bearer $fabricToken" -d "$connectToGitBody" "$initializeConnectionUrl")

    # Send the GET request
    GetUrl="$baseUrl/workspaces/$Id/git/status"
    GetStatus=$(curl -X GET -H "Content-Type: application/json" -H "Authorization: Bearer $fabricToken" "$GetUrl")

    # Print the response
    echo "GetStatus: $GetStatus"
    # Extracting workspaceHead and remoteCommitHash without jq
    updateFromStatus='{
        "remoteCommitHash": "'$(echo "$GetStatus" | grep -o '"remoteCommitHash":"[^"]*"' | cut -d ":" -f 2 | tr -d '"')'",
        "workspaceHead": "'$(echo "$GetStatus" | grep -o '"workspaceHead":"[^"]*"' | cut -d ":" -f 2 | tr -d '"')'"
    }'



    echo "The Git connection for workspace '$workspaceName' has been successfully initialized."

    requiredAction=$(echo "$initializeConnectionResponse" | grep -o '"requiredAction"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/"requiredAction"[[:space:]]*:[[:space:]]*"\([^"]*\)"/\1/')


    # Extract the response code
    echo " URL: $initializeConnectionUrl"
    echo " status: $initializeConnectionResponse" 
    echo " action: $requiredAction" 


if [ "$requiredAction" = "UpdateFromGit" ]; then

        # Update from Git
        echo "Updating the workspace '$workspaceName' from Git."

        updateFromGitUrl="$baseUrl/workspaces/$Id/git/updateFromGit"

        updateFromGitBody='{
            "requiredAction": "UpdateFromGit",
            "workspaceHead": "'$(echo "$initializeConnectionResponse" | grep -o '"remoteCommitHash":"[^"]*"' | cut -d '"' -f 4)'",
            "remoteCommitHash": "'$(echo "$initializeConnectionResponse" | grep -o '"remoteCommitHash":"[^"]*"' | cut -d ":" -f 2 | tr -d '"')'"            
        }'

        updateFromGitResponse=$(curl -X POST -H "Content-Type: application/json" -H "Authorization: Bearer $fabricToken" -d "$updateFromGitBody" "$updateFromGitUrl")

        echo "details '$updateFromGitBody' from Git."
        echo "Updating response '$updateFromGitResponse' from Git."

        operationId=$(echo "$updateFromGitResponse" | grep -o '"x-ms-operation-id": *"[^"]*"' | sed 's/"x-ms-operation-id": *"\([^"]*\)"/\1/')
        retryAfter=$(echo "$updateFromGitResponse" | grep -o '"Retry-After": *"[^"]*"' | sed 's/"Retry-After": *"\([^"]*\)"/\1/')

        echo "Long Running Operation ID: '$operationId' has been scheduled for updating the workspace '$workspaceName' from Git with a retry-after time of '$retryAfter' seconds."
        
        # Poll Long Running Operation
        getOperationState="$baseUrl/operations/$operationId"
        while true
        do
            #operationState=$(curl -s -H "${fabricHeaders[@]}" "$getOperationState")

            operationState=$(curl -X GET -H "Content-Type: application/json" -H "Authorization: Bearer $fabricToken" "$getOperationState")

            echo "Update from Git operation status: $(echo "$operationState" | grep -o '"Status": *"[^"]*"' | sed 's/"Status": *"\([^"]*\)"/\1/')"

            if [ "$(echo "$operationState" | grep -o '"Status": *"[^"]*"' | sed 's/"Status": *"\([^"]*\)"/\1/')" = "NotStarted" ] || [ "$(echo "$operationState" | grep -o '"Status": *"[^"]*"' | sed 's/"Status": *"\([^"]*\)"/\1/')" = "Running" ]; then
                sleep "$retryAfter"
            else
                break
            fi

        done
    else
        requiredAction=$(echo "$initializeConnectionResponse" | grep -o '"RequiredAction": *"[^"]*"' | sed 's/"RequiredAction": *"\([^"]*\)"/\1/')
        echo "Expected RequiredAction to be UpdateFromGit but found $requiredAction" >&2
    fi


    if [ "$(echo "$operationState" | grep -o '"Status": *"[^"]*"' | sed 's/"Status": *"\([^"]*\)"/\1/')" = "Failed" ]; then
        echo "Failed to update the workspace '$workspaceName' with content from Git. Error response: $(jq -c <<< "$(jq '.Error' <<< "$operationState")")" >&2
    else
        echo "The workspace '$workspaceName' has been successfully updated with content from Git."
    fi


}



