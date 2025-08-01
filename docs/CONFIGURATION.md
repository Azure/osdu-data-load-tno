# Configuration Guide

This document provides comprehensive configuration information for the OSDU Data Load TNO application. 

## Batching Configuration

The following master data types are submitted to the workflow service in batches. The batch size can be configured with OSDU_MasterDataManifestSubmissionBatchSize. The default is 25.

- MiscMasterData 
- Well
- Wellbores;

There are different methods for setting the configuration values:

## Environment Variables

You can also configure using environment variables with the `OSDU_` prefix:

## PowerShell (Windows)
```powershell
$env:OSDU_BaseUrl = "https://your-osdu-instance.com"
$env:OSDU_ClientId = "your-client-id"
$env:OSDU_TenantId = "your-tenant-id"
$env:OSDU_DataPartition = "your-data-partition"
$env:OSDU_LegalTag = "your-legal-tag"
$env:OSDU_AclViewer = "data.default.viewers@{DataPartition}.dataservices.energy"
$env:OSDU_AclOwner = "data.default.owners@{DataPartition}.dataservices.energy"
$env:OSDU_UserEmail = "your-object-id"
$env:OSDU_TestDataUrl = "https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
```

## Command Prompt (Windows)
```cmd
set OSDU_BaseUrl=https://your-osdu-instance.com
set OSDU_ClientId=your-client-id
set OSDU_TenantId=your-tenant-id
set OSDU_DataPartition=your-data-partition
set OSDU_LegalTag=your-legal-tag
set OSDU_AclViewer=data.default.viewers@{DataPartition}.dataservices.energy
set OSDU_AclOwner=data.default.owners@{DataPartition}.dataservices.energy
set OSDU_UserEmail=your-object-id
set OSDU_TestDataUrl=https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip
```

## Bash (Linux/macOS)
```bash
export OSDU_BaseUrl="https://your-osdu-instance.com"
export OSDU_ClientId="your-client-id"
export OSDU_TenantId="your-tenant-id"
export OSDU_DataPartition="your-data-partition"
export OSDU_LegalTag="your-legal-tag"
export OSDU_AclViewer="data.default.viewers@{DataPartition}.dataservices.energy"
export OSDU_AclOwner="data.default.owners@{DataPartition}.dataservices.energy"
export OSDU_UserEmail="your-object-id"
export OSDU_TestDataUrl="https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
```

## appsettings.json
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
    "UserEmail": "your-object-id",
    "RetryCount": 3,
    "RetryDelay": "00:00:02",
    "MasterDataManifestSubmissionBatchSize": 25,
    "TestDataUrl": "https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
  }
}
```

## Visual Studio (launchSettings.json)
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
        "OSDU_UserEmail": "your-object-id",
        "OSDU_TestDataUrl": "https://community.opengroup.org/osdu/data/open-test-data/-/archive/master/open-test-data-master.zip"
      }
    }
  }
}
```
