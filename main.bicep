var shareName = 'open-test-data'
var storageAccountName = uniqueString(resourceGroup().id, deployment().name)
var roleAssignmentName = guid('${resourceGroup().name}contributor')
var acrName = uniqueString(resourceGroup().id, deployment().name)
var managedIdentityName = uniqueString(resourceGroup().id, deployment().name)
var contributorRoleDefinitionId = resourceId('Microsoft.Authorization/roleDefinitions', 'b24988ac-6180-42a0-ab88-20f7382dd24c')
var containerImageName = 'osdu-data-load-tno:latest'
var templateSpecName = 'osdu-data-load-tno'
var templateSpecVersionName = '1.0'

resource userAssignedIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2021-09-30-preview' = {
  name: managedIdentityName
  location: resourceGroup().location
}

resource storage 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: storageAccountName
  location: resourceGroup().location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'

  resource fileService 'fileServices' = {
    name: 'default'

    resource dataShare 'shares' = {
      name: shareName
    }

    resource outputShare 'shares' = {
      name: 'output'
    }
  }
}

resource registry 'Microsoft.ContainerRegistry/registries@2021-06-01-preview' = {
  name: acrName
  location: resourceGroup().location
  sku: {
    name: 'Standard'
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${userAssignedIdentity.id}': {}
    }
  }
  properties: {
    anonymousPullEnabled: true
  }
}

resource roleAssignment  'Microsoft.Authorization/roleAssignments@2020-10-01-preview' = {
  name: roleAssignmentName
  scope: resourceGroup()
  properties: {
    description: 'Managed identity access for the RG'
    roleDefinitionId: contributorRoleDefinitionId
    principalId: userAssignedIdentity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}

resource uploadDeploymentScript 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'fileshare-load'
  location: resourceGroup().location
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${userAssignedIdentity.id}': {}
    }
  }
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT2H'
    retentionInterval: 'PT1H'
    cleanupPreference: 'OnSuccess'
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
        value: 'open-test-data'
      }
    ]
    scriptContent: '''
      #!/bin/bash
      set -e
      LOG=script_log.txt
      FILE_NAME=open-test-data.gz
      SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
      DATA_DIR="/tmp/${AZURE_STORAGE_SHARE}"

      echo -e "Retrieving data from OSDU..." 2>&1 | tee -a $LOG
      wget -O $FILE_NAME https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data/-/archive/Azure/M8/open-test-data-Azure-M8.tar.gz 2>&1 | tee -a $LOG

      # Create Directory structure
      echo -e "Creating Directory structure..." 2>&1 | tee -a $LOG
      mkdir -p $DATA_DIR/datasets/documents
      mkdir -p $DATA_DIR/datasets/markers
      mkdir -p $DATA_DIR/datasets/trajectories
      mkdir -p $DATA_DIR/datasets/well-logs
      mkdir -p $DATA_DIR/schema
      mkdir -p $DATA_DIR/templates
      mkdir -p $DATA_DIR/TNO/contrib
      mkdir -p $DATA_DIR/TNO/provided
      ls -l $DATA_DIR | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/datasets/documents --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/USGS_docs
      echo -e "Extracted Dataset Documents" 2>&1 | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/datasets/markers --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/markers
      echo -e "Extracted Dataset Markers" 2>&1 | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/datasets/trajectories --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/trajectories
      echo -e "Extracted Dataset Trajectories" 2>&1 | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/datasets/well-logs --strip-components=5 open-test-data-Azure-M8/rc--1.0.0/1-data/3-provided/well-logs   
      echo -e "Extracted Dataset Well Logs" 2>&1 | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/schema --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/3-schema
      echo -e "Extracted Schemas" 2>&1 | tee -a $LOG
      
      tar -xzvf $FILE_NAME -C $DATA_DIR/templates --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/5-templates
      echo -e "Extracted Templates" 2>&1 | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/TNO/contrib --strip-components=5 open-test-data-Azure-M8/rc--3.0.0/1-data/3-provided/TNO
      echo -e "Extracted TNO Contrib" 2>&1 | tee -a $LOG

      tar -xzvf $FILE_NAME -C $DATA_DIR/TNO/provided --strip-components=3 open-test-data-Azure-M8/rc--3.0.0/4-instances/TNO
      echo -e "Extracted TNO Provided" 2>&1 | tee -a $LOG

      # Upload to Azure Storage
      echo -e "Files Uploading..." 2>&1 | tee -a $LOG
      az storage file upload-batch --destination $AZURE_STORAGE_SHARE --source $DATA_DIR | tee -a $LOG
      echo -e "Upload Complete" 2>&1 | tee -a $LOG

      echo '{"status": {"download": "Success", "extract": "Success", "upload": "Success"}}' | jq > $AZ_SCRIPTS_OUTPUT_PATH
    '''
  }
}

resource acrDockerImage 'Microsoft.Resources/deploymentScripts@2020-10-01' = {
  name: 'build-and-push-image'
  location: resourceGroup().location
  kind: 'AzureCLI'
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${userAssignedIdentity.id}': {}
    }
  }
  properties: {
    azCliVersion: '2.37.0'
    timeout: 'PT2H'
    retentionInterval: 'PT1H'
    cleanupPreference: 'OnSuccess'
    environmentVariables: [
      {
        name: 'CONTAINER_IMAGE_NAME'
        value: containerImageName
      }
      {
        name: 'REGISTRY_NAME'
        value: acrName
      }
      {
        name: 'AZURE_TENANT'
        value: subscription().tenantId
      }
    ]
    scriptContent: '''
      #!/bin/bash
      set -e
      LOG=script_log.txt
      FILE_NAME=main.zip
      SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
      DATA_DIR="/tmp/osdu-data-load-tno-main"

      echo -e "Retrieving Source..." 2>&1 | tee -a $LOG
      wget -O $FILE_NAME https://github.com/Azure/osdu-data-load-tno/archive/refs/heads/main.zip 2>&1 | tee -a $LOG
      unzip $FILE_NAME -d /tmp
      echo -e "Extracted Source" 2>&1 | tee -a $LOG

      echo -e "Build and Import Image: ${CONTAINER_IMAGE_NAME} into ACR: ${REGISTRY_NAME}" 2>&1 | tee -a $LOG
      az acr build --build-arg AZURE_TENANT=$AZURE_TENANT --image ${CONTAINER_IMAGE_NAME} --registry ${REGISTRY_NAME} --file ${DATA_DIR}/Dockerfile $DATA_DIR | tee -a $LOG

      result=$(az acr repository list -n ${REGISTRY_NAME} -ojson)
      echo $result | jq -c > $AZ_SCRIPTS_OUTPUT_PATH
    '''
  }
}

resource createTemplateSpec 'Microsoft.Resources/templateSpecs@2021-05-01' = {
  name: templateSpecName
  location: resourceGroup().location
  properties: {
    description: 'Load OSDU with Open Test Data.'
    displayName: 'Open Test Data Load'
  }
}

resource createTemplateSpecVersion 'Microsoft.Resources/templateSpecs/versions@2021-05-01' = {
  parent: createTemplateSpec
  name: templateSpecVersionName
  location: resourceGroup().location
  properties: {
    mainTemplate: {
      '$schema': 'https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#'
      'contentVersion': '1.0.0.0'
      'parameters': {
        'endpoint': {
          'type': 'string'
          'metadata': {
            'description': 'FQDN Endpoint ie: myosdu.mydomain.com'
          }
        }
        'dataPartition': {
          'type': 'string'
          'metadata': {
            'description': 'Data Partition name ie: mypartition'
          }
        }
        'viewerGroup': {
          'type': 'string'
          'metadata': {
            'description': 'Reader Group ie: data.default.viewers@contoso.com'
          }
        }
        'ownerGroup': {
          'type': 'string'
          'metadata': {
            'description': 'Owner Group ie: data.default.owners@contoso.com'
          }
        }
        'legalTag': {
          'type': 'string'
          'metadata': {
            'description': 'Legal Tag Name ie: legal-tag-load'
          }
        }
        'clientId': {
          'type': 'string'
          'metadata': {
            'description': 'Client Id.'
          }
        }
        'clientSecret': {
          'type': 'string'
          'metadata': {
            'description': 'Client Secret.'
          }
        }
      }
      'variables': {
        'acrName': acrName
        'imageName': containerImageName
      }
      'resources': [
        {
          'type': 'Microsoft.ContainerInstance/containerGroups'
          'apiVersion': '2021-09-01'
          'name': '[concat(\'data-load-\', parameters(\'name\'))]'
          'location': '[parameters(\'location\')]'
          'properties': {
            'containers': [
              {
                'name': 'deploy'
                'properties': {
                  'image': '[concat(variables(\'acrName\'), \'.azurecr.io/\', variables(\'imageName\'))]'
                  'command': [ ]
                  'environmentVariables': [
                    {
                      'name': 'OSDU_ENDPOINT'
                      'value': '[concat(\'https://\'), parameters(\'endpoint\')]'
                    }
                    {
                      'name': 'DATA_PARTITION'
                      'value': '[parameters(\'dataPartition\')]'
                    }
                    {
                      'name': 'VIEWER_GROUP'
                      'value': '[parameters(\'viewerGroup\')]'
                    }
                    {
                      'name': 'OWNER_GROUP'
                      'value': '[parameters(\'ownerGroup\')]'
                    }
                    {
                      'name': 'LEGAL_TAG'
                      'value': '[parameters(\'legalTag\')]'
                    }
                    {
                      'name': 'CLIENT_ID'
                      'value': '[parameters(\'clientId\')]'
                    }
                    {
                      'name': 'CLIENT_SECRET'
                      'value': '[parameters(\'clientSecret\')]'
                    }
                  ]
                  'ports': [ ]
                  'resources': {
                    'requests': {
                      'cpu': 4
                      'memoryInGB': 16
                    }
                  }
                }
              }
            ]
            'osType': 'Linux'
            'restartPolicy': 'Never'
          }
        }
      ]
    }
  }
}


output scriptLogs string = reference('${uploadDeploymentScript.id}/logs/default', uploadDeploymentScript.apiVersion, 'Full').properties.log
