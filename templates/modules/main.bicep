targetScope = 'subscription'

@description('Resource Group Prefix')
param resourceGroupName string = 'osdu-data-load-tno'

@description('Resource Group Location')
param resourceGroupLocation string = 'eastus'


// Create Resource Group
resource resourceGroup 'Microsoft.Resources/resourceGroups@2021-01-01' = {
  name: resourceGroupName
  location: resourceGroupLocation
}

// Module Storage Account with Data Copy
module blob 'modules/storage.bicep' = {
  name: 'labStorage'
  scope: resourceGroup
  params: {
    location: resourceGroupLocation
  }
}
