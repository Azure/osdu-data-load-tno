# OSDU Data Load TNO - C# Implementation

A modern C# application for loading TNO (Netherlands Organisation for Applied Scientific Research) data into the OSDU platform. This tool provides **100% functional equivalency** with the original Python solution while offering improved performance, reliability, and maintainability.

## ✨ Key Features

- 🚀 **Simple CLI Interface** - Three intuitive commands to get you started
- 🔄 **Automatic Processing** - Handles all TNO data types in the correct dependency order
- 📁 **File Upload Support** - Complete 4-step OSDU file upload workflow
- 🔧 **Smart Batching** - Automatically splits large datasets to meet OSDU limits
- 🔐 **Secure Authentication** - Uses Azure Identity for passwordless authentication
- 📊 **Progress Tracking** - Real-time progress updates and detailed logging
- 🛡️ **Error Resilience** - Comprehensive retry policies and error handling
- 🏗️ **Clean Architecture**: CQRS pattern with proper separation of concerns

## 🚀 Quick Start

### 1. Prerequisites

Before you begin, ensure you have:

- **.NET 9.0** or later installed
- **Azure CLI** for authentication: `az login --tenant your-tenant-id`
- [**Azure Developer CLI (azd)** for deployments](https://learn.microsoft.com/en-us/azure/developer/azure-developer-cli/install-azd).
- **OSDU Platform Access** with `users.datalake.ops` role
- **Visual Studio** or **VS Code** (optional, for development)

### 2. Configure the Application

Update `appsettings.json` in the `src/OSDU.DataLoad.Console/` directory with your OSDU instance details:

```json
{
  "Osdu": {
    "BaseUrl": "https://your-osdu-instance.com",
    "TenantId": "your-tenant-id",
    "ClientId": "your-client-id", 
    "DataPartition": "your-data-partition",
    "LegalTag": "{DataPartition}-your-legal-tag",
    "AclViewer": "data.default.viewers@{DataPartition}.dataservices.energy",
    "AclOwner": "data.default.owners@{DataPartition}.dataservices.energy",
    "UserEmail": "your-object-id" // Optional - assigns use users.datalake.ops if set
  }
}
```

**Note**: You can provide environment variables instead. See: **[Configuration Guide](docs/CONFIGURATION.md)**

### 3. Build and Run

```bash
# Navigate to the console project
cd src/OSDU.DataLoad.Console

# Build the solution
dotnet build

# Run commands directly
dotnet run -- help
dotnet run -- download-tno --destination "~/osdu-data/tno"
dotnet run -- load --source "~/osdu-data/tno"
```
## 📋 Available Commands

### Default Behavior (No Arguments)
```bash
# Run without any arguments - downloads data if needed, then loads it
dotnet run
```
When run without arguments, the application will:
1. Check for TNO data in `~/osdu-data/tno/` (user home directory)
2. Download the test data if not present (~2.2GB)
3. Load all data types into OSDU platform automatically

This is the **easiest way to get started** - just configure your OSDU settings and run!

### Help Command
```bash
# From console project directory (recommended)
dotnet run -- help

# Or from src directory
dotnet run --project OSDU.DataLoad.Console --working-directory OSDU.DataLoad.Console -- help
```
Shows available commands, usage examples, and current configuration status.

### Download TNO Test Data
```bash
# Download ~2.2GB of official test data (from console project directory)
dotnet run -- download-tno --destination "~/osdu-data/tno"

# Overwrite existing data
dotnet run -- download-tno --destination "~/osdu-data/tno" --overwrite
```

### Load Data
```bash
# Load all TNO data types in dependency order (from console project directory)
dotnet run -- load --source "~/osdu-data/tno"
```

The application automatically processes all data types in the correct order:
1. Reference Data → Misc Master Data → Wells → Wellbores → Documents → Well Logs → Well Markers → Wellbore Trajectories → Work Products

## 📊 What Gets Loaded

| Data Type | Description | File Formats | Processing Method |
|-----------|-------------|--------------|-------------------|
| Wells | Well master data | CSV, JSON, Excel | Records API |
| Wellbores | Wellbore information | CSV, JSON, Excel | Records API |
| Well Logs | Log curve data | LAS, DLIS, CSV | Files + Records API |
| Documents | Document files | PDF, DOC, XLS, etc. | Files + Records API |
| Work Products | Work product metadata | JSON manifests | Records API with file references |
| Reference Data | Lookup tables | CSV, JSON | Records API |
| *...and more* | See [Data Load Process](docs/DATA_LOAD_PROCESS.md) for complete list | | |

## 💡 Expected Directory Structure

The application expects the following directory structure (automatically created by `dotnet run download-tno`):

> **📁 Cross-Platform Default**: `~/osdu-data/tno/` resolves to:
> - **Windows**: `C:\Users\{username}\osdu-data\tno`
> - **Linux**: `/home/{username}/osdu-data/tno`  
> - **macOS**: `/Users/{username}/osdu-data/tno`

```
~/osdu-data/tno/                     # Default location (cross-platform)
├── datasets/                        # File data (Phase 3)
│   ├── documents/                   # Document files
│   ├── markers/                     # Marker data files  
│   ├── trajectories/                # Trajectory data files
│   └── well-logs/                   # Well log files
├── manifests/                       # Generated manifest directories (Phases 1-2)
│   ├── reference-manifests/         # Reference data manifests
│   ├── misc-master-data-manifests/  # Misc master data manifests
│   ├── master-well-data-manifests/  # Well master data manifests
│   └── master-wellbore-data-manifests/ # Wellbore master data manifests
├── TNO/                            # TNO-specific data
│   ├── contrib/                     # TNO contributed data
│   └── provided/
│       └── TNO/
│           └── work-products/       # Work product data (Phase 4)
│               ├── markers/
│               ├── trajectories/
│               ├── well\ logs/      # Note: contains space
│               └── documents/
├── schema/                          # OSDU schema files
└── templates/                       # Data templates
```

## ☁️ Azure Deployments

### Configure Environment

1. Create an azd environment

    ```bash
    # Navigate to the project root
    azd init -e dev
    ```

2. Configure the environment variables

    ```bash
    azd env set OSDU_TenantId $(az account show --query tenantId -o tsv )
    azd env set AZURE_SUBSCRIPTION_ID <Azure subscription id>
    azd env set AZURE_LOCATION <Azure Region>
    azd env set OSDU_BaseUrl <https://your-osdu-instance.com>
    azd env set OSDU_ClientId <your-client-ID>
    azd env set OSDU_DataPartition <your-data-partition>
    azd env set OSDU_LegalTag <{DataPartition}-your-legal-tag>
    azd env set OSDU_AclViewer <data.default.viewers@{DataPartition}.dataservices.energy>
    azd env set OSDU_AclOwner <data.default.owners@{DataPartition}.dataservices.energy>
    ```

### Deploy the Infrastructure

  ```bash
  azd provision
  ```

### Assign managed identity `users.datalake.ops` role

Get the object ID of the managed identity and assign it `users.datalake.ops` on your data partition.

### Deploy the Application

  ```bash
  azd deploy
  ```

## 📚 Additional Resources

For detailed information on specific topics, see our documentation:

- **[Data Loading Process](docs/DATA_LOAD_PROCESS.md)** - Detailed workflow and processing order
- **[Technical Architecture](docs/TECHNICAL_ARCHITECTURE.md)** - CQRS pattern, Clean Architecture, and project structure  
- **[Configuration Guide](docs/CONFIGURATION.md)** - Advanced configuration options and environment variables
- **[Python Comparison](docs/PYTHON_COMPARISON.md)** - How this C# version matches the original Python implementation

---

## 🛠️ Common Issues and Solutions

### 1. Authentication Failures
**Symptoms**: HTTP 401 errors, "Failed to authenticate" messages

**Solutions**:
- **Azure CLI**: Ensure you're logged in: `az login --tenant your-tenant-id`
- **Permissions**: Verify you have the `users.datalake.ops` role in OSDU
- **Configuration**: Check TenantId and ClientId in configuration
- **Managed Identity**: Verify Managed Identity is configured (when running on Azure)
- **Scope**: Ensure the scope is correctly set to `{ClientId}/.default`
- **Environment Variables**: Verify `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` are set correctly

### 2. Performance Issues
**Symptoms**: Slow upload speeds, timeouts

**Solutions**:
- **Batch Size**: Consider reducing batch size (default 500 is optimal)
- **Timeout**: Increase RequestTimeoutMs for large files
- **Network**: Check network connectivity and bandwidth to OSDU platform
- **File Size**: Split large files into smaller chunks

**Solutions**:
- **Batch Size**: Consider reducing batch size (default 500 is optimal)
- **Timeout**: Increase RequestTimeoutMs for large files
- **Network**: Check network connectivity and bandwidth to OSDU platform
- **File Size**: Split large files into smaller chunks

### 3. File Upload - Metadata Issues
**Symptoms**: The file is uploaded and metadata is created, but /v2/records/{id} returns 404
```
fail: OSDU.DataLoad.Infrastructure.Services.OsduHttpClient[0]
      [2e82ab6a] GET https://pm44a0805b33bc4.oep.ppe.azure-int.net/api/storage/v2/records/opendes:dataset--File.Generic:e4f2b1ee-2732-4259-ab47-d30ff4c2a095 failed with status NotFound
fail: OSDU.DataLoad.Infrastructure.Services.OsduHttpClient[0]
      [2e82ab6a] Step 4 Failed: Could not retrieve record version for FileID: opendes:dataset--File.Generic:e4f2b1ee-2732-4259-ab47-d30ff4c2a095
```
**Solutions**:
- Kill the OSDU-Storage pods

## Error Categories and Handling

### 1. Transient Errors (Retry with Exponential Backoff)
- Network timeouts
- HTTP 503 Service Unavailable
- HTTP 429 Too Many Requests
- OAuth2 token expiration
- Temporary OSDU service unavailability

### 2. Permanent Errors (Fail Fast)
- HTTP 400 Bad Request (invalid data format)
- HTTP 401 Unauthorized (invalid credentials)
- HTTP 403 Forbidden (insufficient permissions)
- HTTP 413 Payload Too Large (batch exceeds 500 records) → **Auto-handled by intelligent batching**
- Schema validation failures
- Invalid file formats

### 3. Partial Failures (Continue Processing)
- Some records in batch succeed, others fail
- Some files upload successfully, others fail
- Continue processing remaining batches/files
- Report detailed success/failure metrics per data type

## Error Code Reference

| Error Code | Description | Solution |
|------------|-------------|----------|
| HTTP 400 | Bad Request - Invalid data format | Review field mappings and data validation |
| HTTP 401 | Unauthorized - Authentication failure | Check credentials and OSDU role assignment |
| HTTP 403 | Forbidden - Insufficient permissions | Verify `users.datalake.ops` role |
| HTTP 413 | Payload Too Large | ✅ Auto-handled by intelligent batching |
| HTTP 429 | Too Many Requests | Retry logic handles automatically |
| HTTP 503 | Service Unavailable | Retry logic handles automatically |


## Contributing

This solution follows Clean Architecture and CQRS principles. See our [Technical Architecture](docs/TECHNICAL_ARCHITECTURE.md) guide for detailed information on:

- Adding new features
- Code standards and patterns
- Testing guidelines
- Project structure

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft
trademarks or logos is subject to and must follow
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.

OSDU is a trademark of The Open Group.