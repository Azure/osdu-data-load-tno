

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
      AZURE_STORAGE_SHARE="open-test-data"
      PARENT_DIR="/tmp/${AZURE_STORAGE_SHARE}"

      echo -e "Retrieving data from OSDU..."
      wget -O $FILE_NAME https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data/-/archive/Azure/M8/open-test-data-Azure-M8.tar.gz

      # Extract datasets
      mkdir -p $PARENT_DIR/datasets/documents
      mkdir -p $PARENT_DIR/datasets/markers
      mkdir -p $PARENT_DIR/datasets/trajectories
      mkdir -p $PARENT_DIR/datasets/well-logs
      tar -xzvf $FILE_NAME -C $PARENT_DIR/datasets/documents --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/USGS_docs
      tar -xzvf $FILE_NAME -C $PARENT_DIR/datasets/markers --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/markers
      tar -xzvf $FILE_NAME -C $PARENT_DIR/datasets/trajectories --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/trajectories
      tar -xzvf $FILE_NAME -C $PARENT_DIR/datasets/well-logs --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/well-logs

      # Extract schemas
      mkdir -p $PARENT_DIR/schemas
      tar -xzvf $FILE_NAME -C $PARENT_DIR/schema --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/3-schema

      # Extract Manifests
      mkdir -p $PARENT_DIR/templates
      mkdir -p $PARENT_DIR/TNO/contrib
      mkdir -p $PARENT_DIR/TNO/provided
      tar -xzvf $FILE_NAME -C $PARENT_DIR/schema --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/3-schema
      tar -xzvf $FILE_NAME -C $PARENT_DIR/templates --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/5-templates
      tar -xzvf $FILE_NAME -C $PARENT_DIR/TNO/contrib --strip-components=5 open-test-data-Azure-M8/rc--3.0.0/1-data/3-provided/TNO
      tar -xzvf $FILE_NAME -C $PARENT_DIR/TNO/provided --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/4-instances/TNO

      # Upload to Azure Storage
      az storage file upload-batch \
        --account-name $AZURE_STORAGE_ACCOUNT \
        --account-key $AZURE_STORAGE_KEY \
        --destination $AZURE_STORAGE_SHARE \
        --source $PARENT_DIR

    '''
  }
}


output storageAccountName string = storageAccountName
