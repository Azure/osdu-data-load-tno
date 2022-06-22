

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

      echo -e "Copying Documents"
      mkdir -p /tmp/open-test-data/documents
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/documents --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/USGS_docs
      az storage file upload-batch \
        --destination $AZURE_STORAGE_SHARE \
        --source /tmp/open-test-data \
        --pattern "open-test-data/documents/**"

      mkdir -p /tmp/open-test-data/datasets/markers
      mkdir -p /tmp/open-test-data/datasets/trajectories
      mkdir -p /tmp/open-test-data/datasets/well-logs
      mkdir -p /tmp/open-test-data/schema
      mkdir -p /tmp/open-test-data/schema
      mkdir -p /tmp/open-test-data/templates
      mkdir -p /tmp/open-test-data/TNO/contrib
      mkdir -p /tmp/open-test-data/TNO/provided

      echo -e "Extracting Files..."

      tar -xzvf $FILE_NAME -C /tmp/open-test-data/datasets/markers --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/markers
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/datasets/trajectories --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/trajectories
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/datasets/well-logs --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/well-logs
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/schema --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/3-schema
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/templates --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/5-templates
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/TNO/contrib --strip-components=5 open-test-data-Azure-M8/rc--3.0.0/1-data/3-provided/TNO
      tar -xzvf $FILE_NAME -C /tmp/open-test-data/TNO/provided --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/4-instances/TNO

      echo -e "Uploading Files..."
      az storage file upload-batch \
        --destination $AZURE_STORAGE_SHARE \
        --source /tmp/open-test-data \
        --pattern "open-test-data/**"
    '''
  }
}


output storageAccountName string = storageAccountName
