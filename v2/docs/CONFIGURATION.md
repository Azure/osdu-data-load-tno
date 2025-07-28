# Configuration Guide

This document provides comprehensive configuration information for the OSDU Data Load TNO application.

## Basic Configuration

### appsettings.json
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft": "Warning",
      "Microsoft.Hosting.Lifetime": "Information"
    }
  },
  "Osdu": {
    "BaseUrl": "https://your-osdu-instance.com",
    "TenantId": "your-tenant-id", 
    "ClientId": "your-client-id",
    "DataPartition": "your-data-partition",
    "LegalTag": "osdu-tno-data",
    "AclViewer": "data.default.viewers@{DataPartition}.dataservices.energy",
    "AclOwner": "data.default.owners@{DataPartition}.dataservices.energy",
    "RetryCount": 3,
    "RetryDelay": "00:00:02",
    "BatchSize": 500,
    "RequestTimeoutMs": 30000,
    "TestDataUrl": "https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
  }
}
```

**Note**: The `AuthScope` is automatically generated as `{ClientId}/.default` and doesn't need to be configured manually.
**Note**: No client secret is required. The application uses DefaultAzureCredential which automatically determines the scope as `{ClientId}/.default`.

## Configuration Properties

| Property | Description | Default Value | Environment Variable |
|----------|-------------|---------------|---------------------|
| `BaseUrl` | OSDU platform base URL | _(required)_ | `OSDU_BaseUrl` |
| `TenantId` | Azure Active Directory tenant ID | _(required)_ | `OSDU_TenantId` |
| `ClientId` | Azure Active Directory client ID | _(required)_ | `OSDU_ClientId` |
| `DataPartition` | OSDU data partition | _(required)_ | `OSDU_DataPartition` |
| `LegalTag` | Legal tag for data compliance | _(required)_ | `OSDU_LegalTag` |
| `AclViewer` | ACL viewer email/group | _(required)_ | `OSDU_AclViewer` |
| `AclOwner` | ACL owner email/group | _(required)_ | `OSDU_AclOwner` |
| `RetryCount` | Number of retry attempts | `3` | `OSDU_RetryCount` |
| `RetryDelay` | Delay between retries | `00:00:02` | `OSDU_RetryDelay` |
| `BatchSize` | Records per batch | `500` | `OSDU_BatchSize` |
| `RequestTimeoutMs` | HTTP request timeout | `30000` | `OSDU_RequestTimeoutMs` |
| `TestDataUrl` | URL for downloading test data | [OSDU Open Test Data](https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip) | `OSDU_TestDataUrl` |
| `AuthScope` | OAuth2 scope _(auto-generated)_ | `{ClientId}/.default` | _(not configurable)_ |


## Environment Variables

You can also configure using environment variables with the `OSDU_` prefix:

### PowerShell (Windows)
```powershell
$env:OSDU_BaseUrl = "https://your-osdu-instance.com"
$env:OSDU_ClientId = "your-client-id"
$env:OSDU_TenantId = "your-tenant-id"
$env:OSDU_DataPartition = "your-data-partition"
$env:OSDU_LegalTag = "your-legal-tag"
$env:OSDU_AclViewer = "data.default.viewers@{DataPartition}.dataservices.energy"
$env:OSDU_AclOwner = "data.default.owners@{DataPartition}.dataservices.energy"
$env:OSDU_TestDataUrl = "https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
```

### Command Prompt (Windows)
```cmd
set OSDU_BaseUrl=https://your-osdu-instance.com
set OSDU_ClientId=your-client-id
set OSDU_TenantId=your-tenant-id
set OSDU_DataPartition=your-data-partition
set OSDU_LegalTag=your-legal-tag
set OSDU_AclViewer=data.default.viewers@{DataPartition}.dataservices.energy
set OSDU_AclOwner=data.default.owners@{DataPartition}.dataservices.energy
set OSDU_TestDataUrl=https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip
```

### Bash (Linux/macOS)
```bash
export OSDU_BaseUrl="https://your-osdu-instance.com"
export OSDU_ClientId="your-client-id"
export OSDU_TenantId="your-tenant-id"
export OSDU_DataPartition="your-data-partition"
export OSDU_LegalTag="your-legal-tag"
export OSDU_AclViewer="data.default.viewers@{DataPartition}.dataservices.energy"
export OSDU_AclOwner="data.default.owners@{DataPartition}.dataservices.energy"
export OSDU_TestDataUrl="https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
```

### Visual Studio (launchSettings.json)
Add to `Properties/launchSettings.json`:
```json
{
  "profiles": {
    "OSDU.DataLoad.Console": {
      "commandName": "Project",
      "environmentVariables": {
        "OSDU_BaseUrl": "https://your-osdu-instance.com",
        "OSDU_ClientId": "your-client-id",
        "OSDU_TenantId": "your-tenant-id",
        "OSDU_DataPartition": "your-data-partition",
        "OSDU_LegalTag": "your-legal-tag",
        "OSDU_AclViewer": "data.default.viewers@{DataPartition}.dataservices.energy",
        "OSDU_AclOwner": "data.default.owners@{DataPartition}.dataservices.energy",
        "OSDU_TestDataUrl": "https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
      }
    }
  }
}
```
