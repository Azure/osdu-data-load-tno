# Technical Architecture

This document provides detailed technical information about the OSDU Data Load TNO application architecture.

## Architecture Overview

The solution follows Clean Architecture patterns with Command Query Responsibility Segregation (CQRS) using MediatR:

### 🏗️ Layer Structure
- **Domain** (`OSDU.DataLoad.Domain`): Core entities and interfaces
- **Application** (`OSDU.DataLoad.Application`): CQRS commands, queries, and handlers
- **Infrastructure** (`OSDU.DataLoad.Infrastructure`): External services and data access
- **Console** (`OSDU.DataLoad.Console`): Command-line interface

### 🔄 CQRS Pattern
All business operations are implemented as:
- **Commands**: Operations that change state (LoadAllData, LoadData, Generate, Transform, Upload)
- **Queries**: Operations that retrieve data (Discover, Validate, GetSchema, RecordExists)
- **Handlers**: Process commands and queries with full separation of concerns

## Project Structure

```
OSDU.DataLoad.sln
├── OSDU.DataLoad.Domain/
│   ├── Entities/
│   │   ├── CoreEntities.cs          # Core domain entities (LoadResult, OsduConfiguration, DataRecord, etc.)
│   │   ├── LoadProgress.cs          # Progress tracking
│   │   ├── FileUploadResult.cs      # File upload response
│   │   ├── UploadUrlResponse.cs     # OSDU upload URL response
│   │   └── FileMetadata.cs          # File metadata for OSDU
│   └── Interfaces/
│       ├── IOsduClient.cs           # OSDU API contract (includes file upload)
│       ├── IDataTransformer.cs      # Data transformation contract
│       ├── IFileProcessor.cs        # File operations contract
│       ├── IManifestGenerator.cs    # Manifest operations contract
│       ├── IDataLoadOrchestrator.cs # Orchestration contract (includes file upload)
│       └── IRetryPolicy.cs          # Retry logic contract
├── OSDU.DataLoad.Application/
│   ├── Commands/
│   │   ├── LoadAllDataCommand.cs    # Load all data types in order
│   │   ├── LoadDataCommand.cs       # Load data from directory
│   │   ├── LoadFromManifestCommand.cs # Load from manifest file
│   │   ├── GenerateManifestCommand.cs # Generate manifest
│   │   ├── SaveManifestCommand.cs   # Save manifest to file
│   │   ├── TransformDataCommand.cs  # Transform source data
│   │   ├── UploadRecordsCommand.cs  # Upload records to OSDU
│   │   └── UploadFilesCommand.cs    # Upload files to OSDU (4-step workflow)
│   ├── Queries/
│   │   ├── DiscoverFilesQuery.cs    # Find source files
│   │   ├── ValidateSourceQuery.cs   # Validate source data
│   │   ├── ValidateManifestQuery.cs # Validate manifest
│   │   ├── RecordExistsQuery.cs     # Check record existence
│   │   └── GetSchemaQuery.cs        # Get OSDU schema
│   └── Handlers/
│       ├── [Command]Handler.cs      # Command handlers
│       └── [Query]Handler.cs        # Query handlers
│   └── Services/
│       ├── DataLoadOrchestrator.cs  # Orchestration service with file handling
│       └── ManifestGenerator.cs     # Manifest operations
├── OSDU.DataLoad.Infrastructure/
│   └── Services/
│       ├── OsduHttpClient.cs        # Complete OSDU API client with file upload
│       ├── TnoDataTransformer.cs    # TNO data transformation
│       ├── FileProcessor.cs         # File operations
│       └── ExponentialRetryPolicy.cs # Retry logic with jitter
└── OSDU.DataLoad.Console/
    ├── Program.cs                   # Entry point with DI setup
    ├── DataLoadApplication.cs       # CLI interface
    └── appsettings.json            # Configuration
```

## Command and Query Architecture

### Primary Commands

#### LoadAllDataCommand
**Purpose**: Orchestrates the complete data loading process from a source directory, automatically processing all TNO data types in the correct dependency order.

**Flow**:
```
LoadAllDataCommand → [For Each Data Type in Order] → LoadDataCommand → DiscoverFilesQuery → GenerateManifestCommand → LoadFromManifestCommand
```

**Parameters**:
- `SourcePath`: Root directory containing TNO data subdirectories

**Error Handling**:
- Invalid source path → Clear error message
- Missing subdirectory → Skip data type with warning
- Authentication failure → Detailed OAuth2 error information
- Phase failure → Continue with next data type, report aggregated results

#### LoadDataCommand (Internal)
**Purpose**: Loads a specific data type from a given directory (used internally by LoadAllDataCommand).

**Flow**:
```
LoadDataCommand → DiscoverFilesQuery → GenerateManifestCommand → LoadFromManifestCommand
```

**Parameters**:
- `SourcePath`: Directory containing specific TNO data type files
- `DataType`: Type of TNO data (Wells, Wellbores, etc.)

#### LoadFromManifestCommand
**Purpose**: Loads data using a pre-generated manifest file with intelligent file upload integration.

**Flow**:
```
LoadFromManifestCommand → [File Upload for certain types] → [TransformDataCommand × N] → UploadRecordsCommand
```

**Process**:
1. Parse manifest to determine data type
2. **Handle file uploads** for file-based data types (Documents, WellLogs, WellMarkers, WellboreTrajectories, WorkProducts)
3. Transform each source file to OSDU format
4. Collect all transformed records
5. Upload in batches (500 records max) to OSDU platform

**File Upload Integration**:
- **Documents** → Files uploaded first, then metadata records
- **WellLogs** → LAS/DLIS files uploaded, then log records with file references
- **WellMarkers** → Associated files uploaded if present
- **WellboreTrajectories** → Trajectory files uploaded if present
- **WorkProducts** → Product files uploaded, then work product records

#### UploadFilesCommand
**Purpose**: Handles complete file upload workflow matching Python equivalency.

**Flow** (matches Python `load_single_file` function):
```
UploadFilesCommand → Get Upload URL → Upload to Blob → Post Metadata → Get Record Version
```

**Process**:
1. **Get upload URL** from OSDU File API (`/api/file/v2/files/uploadURL`)
2. **Upload to Azure Blob Storage** using signed URL
3. **Post file metadata** to OSDU (`/api/file/v2/files/metadata`) 
4. **Get record version** from storage API (`/api/storage/v2/records/{fileId}/versions`)
5. **Return file location data** (file_id, file_source, file_record_version)

#### UploadRecordsCommand  
**Purpose**: Uploads data records to OSDU with intelligent batching.

**Flow**:
```
UploadRecordsCommand → [Split into 500-record batches] → [Upload each batch] → Aggregate Results
```

**Intelligent Batching**:
- **Batch Size**: 500 records (OSDU platform limit)
- **Auto-splitting**: Large datasets automatically split into compliant batches
- **Result Aggregation**: Individual batch results combined into single LoadResult
- **Error Handling**: Failed batches don't prevent processing of remaining batches

**Example**: 929 records → Batch 1 (500 records) + Batch 2 (429 records)

### Supporting Queries

#### DiscoverFilesQuery
**Purpose**: Finds and catalogs source files in a directory.

**Process**:
1. Scan directory recursively
2. Filter by supported file extensions for data type
3. Create SourceFile entities with metadata
4. Return array of discovered files

**Supported Extensions by Data Type**:
- **Wells**: .csv, .json, .xlsx
- **WellLogs**: .las, .dlis, .csv
- **WellboreTrajectories**: .csv, .json
- **Documents**: .pdf, .csv, .txt
- **All Others**: .csv, .json, .xlsx

#### GetSchemaQuery
**Purpose**: Retrieves OSDU schema definitions for data validation.

**Process**:
1. Call OSDU Schema Service (`/api/schema-service/v1/schema/{kind}`)
2. Cache schema definitions for performance
3. Return schema for validation use

#### RecordExistsQuery
**Purpose**: Checks if a record already exists in OSDU platform.

**Process**:
1. Query OSDU Search API with record ID
2. Return boolean indicating existence
3. Used for duplicate prevention  

## OSDU API Integration

### OSDU API Endpoints Reference

The application integrates with the following OSDU Platform APIs:

#### File Service API (`/api/file/v2`)
- `GET /files/uploadURL` - Request signed upload URL for file
- `POST /files/metadata` - Submit file metadata after upload

#### Storage Service API (`/api/storage/v2`)
- `PUT /records` - Upload record batch (max 500 records)
- `GET /records/{id}/versions` - Get record version information
- `GET /records/{recordId}` - Check if individual record exists

#### Schema Service API (`/api/schema-service/v1`)
- `GET /schema/{kind}` - Retrieve schema definition for data kind

#### Azure Blob Storage
- `PUT {signedUrl}` - Upload file to Azure Blob Storage (signed URL from File API)

#### Authentication
- Uses Azure Identity with Bearer token authentication
- Supports refresh token flow for long-running operations

#### Batch Processing Limits
- **File Upload**: No specific limit, but individual file size limits apply
- **Record Upload**: Maximum 500 records per batch
- **Schema Validation**: Performed per record kind

## Dependencies and Versions

The application is built on **.NET 9.0** with the following key dependencies:

### Core Dependencies
- **MediatR** (13.0.0) - CQRS pattern implementation
- **Microsoft.Extensions.Hosting** (9.0.0) - Application hosting and DI
- **Microsoft.Extensions.Configuration** (9.0.0) - Configuration management
- **Microsoft.Extensions.Logging** (9.0.0) - Structured logging

### Azure Integration
- **Azure.Identity** (1.14.2) - Azure authentication
- **Azure.Storage.Blobs** (12.25.0) - Azure Blob Storage client

### Data Processing
- **System.Text.Json** (9.0.0) - JSON serialization
- **Microsoft.Office.Interop.Excel** (Optional) - Excel file processing

## Contributing

This solution follows Clean Architecture and CQRS principles. When adding new features:

1. **Add new entities** to the `Domain.Entities` layer
2. **Define interfaces** in the `Domain.Interfaces` layer
3. **Create commands/queries** in the `Application.Commands`/`Application.Queries` layer
4. **Implement handlers** in the `Application.Handlers` layer
5. **Add infrastructure** implementations in the `Infrastructure.Services` layer
6. **Update CLI interface** in the `Console` layer for new commands

### Code Standards
- Use **record types** for immutable commands and queries
- Implement **proper error handling** with specific exceptions
- Add **comprehensive logging** at appropriate levels
- Include **unit tests** for all handlers
- Follow **async/await** patterns consistently
- Use **CancellationTokens** for all async operations

## Future Enhancements

The current architecture provides a solid foundation for additional features:

### Multi-threading Support
The CQRS architecture could support parallel processing:

```csharp
// Example: Enhanced file upload with parallel processing
// Would require extending current UploadFilesCommand structure
public async Task<LoadResult> ProcessFilesInParallel(IEnumerable<SourceFile> files)
{
    var uploadTasks = files.Select(file => 
        _osduClient.UploadFileAsync(file.FilePath, file.FileName, "description"));
        
    var results = await Task.WhenAll(uploadTasks);
    return AggregateResults(results);
}
```

### Background Processing
Long-running operations could be moved to background services:

```csharp
// Example: Background data loading service
public class DataLoadBackgroundService : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Process queued load operations
        // Implementation would require additional queue infrastructure
    }
}
```

### Enhanced Progress Reporting
Real-time progress updates could be added via MediatR pipeline behaviors:

```csharp
// Example: Progress tracking pipeline behavior
public class ProgressTrackingBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
{
    public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
    {
        // Track operation progress
        // Would require implementing progress reporting infrastructure
        return await next();
    }
}
```

**Note**: These enhancements are conceptual examples and would require additional implementation work.
