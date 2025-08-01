@description('The location used for all deployed resources')
param location string = resourceGroup().location

@description('Tags that will be applied to all resources')
param tags object = {}


param osduDataloadConsoleExists bool

@description('Id of the user or app to assign application roles')
param principalId string

param TenantId string

param AclOwner string

param AclViewer string

param BaseUrl string

param ClientId string

param DataPartition string

param LegalTag string

param UserObjectId string = ''

var abbrs = loadJsonContent('./abbreviations.json')
var resourceToken = uniqueString(subscription().id, resourceGroup().id, location)

// Monitor application with Azure Monitor
module monitoring 'br/public:avm/ptn/azd/monitoring:0.1.0' = {
  name: 'monitoring'
  params: {
    logAnalyticsName: '${abbrs.operationalInsightsWorkspaces}${resourceToken}'
    applicationInsightsName: '${abbrs.insightsComponents}${resourceToken}'
    applicationInsightsDashboardName: '${abbrs.portalDashboards}${resourceToken}'
    location: location
    tags: tags
  }
}

// Container registry
module containerRegistry 'br/public:avm/res/container-registry/registry:0.1.1' = {
  name: 'registry'
  params: {
    name: '${abbrs.containerRegistryRegistries}${resourceToken}'
    location: location
    tags: tags
    publicNetworkAccess: 'Enabled'
    roleAssignments:[
      {
        principalId: osduDataloadConsoleIdentity.outputs.principalId
        principalType: 'ServicePrincipal'
        roleDefinitionIdOrName: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
      }
    ]
  }
}

// Container apps environment
module containerAppsEnvironment 'br/public:avm/res/app/managed-environment:0.4.5' = {
  name: 'container-apps-environment'
  params: {
    logAnalyticsWorkspaceResourceId: monitoring.outputs.logAnalyticsWorkspaceResourceId
    name: '${abbrs.appManagedEnvironments}${resourceToken}'
    location: location
    zoneRedundant: false
    workloadProfiles: [
      {
        workloadProfileType: 'E4'
        name: 'memory'
        minimumCount: 0
        maximumCount: 5
      }
    ]
  }
}

module osduDataloadConsoleIdentity 'br/public:avm/res/managed-identity/user-assigned-identity:0.2.1' = {
  name: 'osduDataloadConsoleidentity'
  params: {
    name: '${abbrs.managedIdentityUserAssignedIdentities}osduDataloadConsole-${resourceToken}'
    location: location
  }
}

module osduDataloadConsoleFetchLatestImage './modules/fetch-container-image.bicep' = {
  name: 'osduDataloadConsole-fetch-image'
  params: {
    exists: osduDataloadConsoleExists
    name: 'osdu-dataload-console'
  }
}

module osduDataloadConsole 'br/public:avm/res/app/container-app:0.8.0' = {
  name: 'osduDataloadConsole'
  params: {
    name: 'osdu-dataload-console'
    disableIngress: true
    scaleMinReplicas: 1
    scaleMaxReplicas: 1
    workloadProfileName: 'memory'
    secrets: {
      secureList:  [
      ]
    }
    // Add volumes for larger storage
    volumes: [
      {
        name: 'temp-storage'
        storageType: 'EmptyDir'
      }
    ]
    containers: [
      {
        image: osduDataloadConsoleFetchLatestImage.outputs.?containers[?0].?image ?? 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'
        name: 'main'
        resources: {
          cpu: json('4.0')
          memory: '32.0Gi'
        }
        // Mount the volume for large file operations
        volumeMounts: [
          {
            volumeName: 'temp-storage'
            mountPath: '/tmp/data'
          }
        ]
        env: [
          {
            name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
            value: monitoring.outputs.applicationInsightsConnectionString
          }
          {
            name: 'AZURE_CLIENT_ID'
            value: osduDataloadConsoleIdentity.outputs.clientId
          }
          // Set temp directory to mounted volume
          {
            name: 'TMPDIR'
            value: '/tmp/data'
          }
          {
            name: 'TEMP'
            value: '/tmp/data'
          }
          {
            name: 'OSDU_TenantId'
            value: TenantId
          }
          {
            name: 'OSDU_AclOwner'
            value: AclOwner
          }
          {
            name: 'OSDU_AclViewer'
            value: AclViewer
          }
          {
            name: 'OSDU_BaseUrl'
            value: BaseUrl
          }
          {
            name: 'OSDU_ClientId'
            value: ClientId
          }
          {
            name: 'OSDU_DataPartition'
            value: DataPartition
          }
          {
            name: 'OSDU_LegalTag'
            value: LegalTag
          }
          {
            name: 'OSDU_UserEmail'
            value: UserObjectId
          }
        ]
      }
    ]
    managedIdentities:{
      systemAssigned: false
      userAssignedResourceIds: [osduDataloadConsoleIdentity.outputs.resourceId]
    }
    registries:[
      {
        server: containerRegistry.outputs.loginServer
        identity: osduDataloadConsoleIdentity.outputs.resourceId
      }
    ]
    environmentResourceId: containerAppsEnvironment.outputs.resourceId
    location: location
    tags: union(tags, { 'azd-service-name': 'osdu-dataload-console' })
  }
}
output AZURE_CONTAINER_REGISTRY_ENDPOINT string = containerRegistry.outputs.loginServer
output AZURE_RESOURCE_OSDU_DATALOAD_CONSOLE_ID string = osduDataloadConsole.outputs.resourceId
