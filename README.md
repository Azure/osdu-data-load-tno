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
- **OSDU Platform Access** with `users.datalake.ops` role
- **Visual Studio** or **VS Code** (optional, for development)

### 2. Configure the Application

Update `appsettings.json` with your OSDU instance details:

```json
{
  "Osdu": {
    "BaseUrl": "https://your-osdu-instance.com",
    "TenantId": "your-tenant-id",
    "ClientId": "your-client-id", 
    "DataPartition": "your-data-partition",
    "LegalTag": "{DataPartition}-your-legal-tag",
    "AclViewer": "data.default.viewers@{DataPartition}.dataservices.energ",
    "AclOwner": "data.default.owners@{DataPartition}.dataservices.energy"
  }
}
```

**Note**: You can provide environment variables instead. See: **[Configuration Guide](docs/CONFIGURATION.md)**

### 3. Build and Run

```bash
# Build the solution
dotnet build

# Show available commands
dotnet run help

# Download test data (optional)
dotnet run download-tno --destination "C:\data\tno"

# Load all data types
dotnet run load --source "C:\data\tno"
```

## 📋 Available Commands

### Help Command
```bash
dotnet run help
```
Shows available commands, usage examples, and current configuration status.

### Download TNO Test Data
```bash
# Download ~2.2GB of official test data
dotnet run download-tno --destination "C:\data\tno"

# Overwrite existing data
dotnet run download-tno --destination "C:\data\tno" --overwrite
```

### Load Data
```bash
# Load all TNO data types in dependency order
dotnet run load --source "C:\data\tno"
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

```
C:\data\tno\
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