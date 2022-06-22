

@description('UTC timestamp used to create distinct deployment scripts for each deployment')
param utcValue string = utcNow()


var shareName = 'open-test-data'
var storageAccountName = uniqueString(resourceGroup().id, deployment().name)


resource storage 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: storageAccountName
  location: resourceGroup().location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'

  resource fileService 'fileServices' = {
    name: 'default'

    resource share 'shares' = {
      name: shareName
    }
  }
}

resource blobDeploymentScript 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'load-blob-${utcValue}'
  location: resourceGroup().location
  kind: 'AzureCLI'
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT10M'
    retentionInterval: 'PT1H'
    environmentVariables: [
      {
        name: 'AZURE_STORAGE_ACCOUNT'
        value: storage.name
      }
      {
        name: 'AZURE_STORAGE_KEY'
        secureValue: storage.listKeys().keys[0].value
      }
      {
        name: 'AZURE_STORAGE_SHARE'
        value: shareName
      }
    ]
    scriptContent: '''
      #!/bin/bash
      set -e
      console.log('Downloading Data')
      FILE_NAME=open-test-data.gz
      wget -O $FILE_NAME https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data/-/archive/Azure/M8/open-test-data-Azure-M8.tar.gz
      console.log('Uploading data')
      az storage file upload -s $FILE_NAME --source $FILE_NAME
    '''
  }
}


output storageAccountName string = storageAccountName
