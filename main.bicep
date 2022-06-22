

@description('UTC timestamp used to create distinct deployment scripts for each deployment')
param utcValue string = utcNow()

@description('Location of Storage Account')
param location string = resourceGroup().location

var shareName = 'open-test-data'
var storageAccountName = uniqueString(resourceGroup().id, deployment().name)


resource storage 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: storageAccountName
  location: location
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
  location: location
  kind: 'AzureCLI'
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT20M'
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
      FILE_NAME=open-test-data.gz

      echo -e "Retrieving data from OSDU..."
      wget -O $FILE_NAME https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data/-/archive/Azure/M8/open-test-data-Azure-M8.tar.gz

      mkdir -p /tmp/open-test-data/documents
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/documents --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/USGS_docs

      echo -e "Copying Documents"
      az storage file upload-batch \
        --account-name $AZURE_STORAGE_ACCOUNT \
        --account-key $AZURE_STORAGE_KEY \
        --destination $AZURE_STORAGE_SHARE \
        --source /tmp/open-test-data \
        --pattern "open-test-data/**"
    '''
  }
}


output storageAccountName string = storageAccountName
