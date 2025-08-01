# OSDU Data Load TNO - Technical Flow Documentation

## 🔄 Detailed Application Flow

This document provides a comprehensive technical overview of the OSDU Data Load TNO application flow, including all components, services, and data transformations.

## Architecture Components

### 1. Entry Point (OSDU.DataLoad.Console)

```
Program.cs
├── ConfigureServices()
│   ├── PathConfiguration (centralized paths)
│   ├── MediatR (command/query pattern)
│   ├── IOsduService (HTTP client wrapper)
│   └── Dependency Injection Container
├── Main() → DataLoadApplication.RunAsync()
└── Exception Handling & Exit Codes
```

### 2. Application Orchestration (DataLoadApplication)

```
DataLoadApplication.RunAsync()
├── Parse Command Line Arguments
├── Route to Command Handler:
│   ├── Default Mode (no args)
│   ├── Load Command (--source)
│   ├── Download Command (--destination)
│   └── Help Command
└── Return Exit Code
```

## Command Processing Flow

### Default Mode Flow
```
HandleDefaultCommand()
├── Set defaultDataPath = ~/osdu-data/tno
├── DisplayConfigurationStatus()
├── CheckIfDataExists(defaultDataPath)
│   ├── Check directories: manifests/, datasets/, TNO/
│   └── Validate manifest files exist
├── If data missing:
│   ├── DownloadDataCommand
│   ├── DisplayDownloadResult()
│   └── Check download success
├── LoadAllDataAsync(defaultDataPath)
└── Return exit code
```

### Load Command Flow
```
HandleLoadCommand(args[])
├── Parse --source argument
├── Validate source path
├── DisplayConfigurationStatus()
├── LoadAllDataAsync(source)
└── Return exit code
```

### Download Command Flow
```
HandleDownloadCommand(args[])
├── Parse --destination and --overwrite arguments
├── Validate destination path
├── DisplayConfigurationStatus()
├── DownloadDataAsync(destination, overwrite)
└── Return exit code
```

## Core Data Loading Process (LoadAllDataAsync)

### Overview
```
LoadAllDataAsync(source)
├── Load OSDU Configuration
├── Step 1: Create Legal Tag
├── Step 2: Generate Manifests
├── Step 3: Upload Files
├── Step 4: Submit Workflow
└── Display Overall Results
```

### Step 1: Legal Tag Creation
```
CreateLegalTagCommand
├── Input: LegalTagName from configuration
├── Handler: CreateLegalTagCommandHandler
│   ├── IOsduService.CreateLegalTagAsync()
│   ├── HTTP POST to /api/legal/v1/legaltags
│   └── Return creation status
├── Output: LoadResult with success/failure
└── Log: Legal tag creation status
```

### Step 2: Manifest Generation
```
GenerateManifestsCommand
├── Input: SourceDataPath, OutputPath, OSDU config
├── Handler: GenerateManifestsCommandHandler
│   ├── Send GenerateWorkProductManifestCommand
│   │   ├── Handler: GenerateWorkProductManifestCommandHandler
│   │   ├── Process work product CSV files
│   │   ├── Call Python script: csv_to_json.py
│   │   ├── Generate JSON manifests
│   │   └── Save to work-product-manifests/
│   ├── Send GenerateNonWorkProductManifestCommand
│   │   ├── Handler: GenerateNonWorkProductManifestCommandHandler
│   │   ├── Process reference data CSV files
│   │   ├── Call Python script: csv_to_json.py
│   │   ├── Generate JSON manifests
│   │   └── Save to non-work-product-manifests/
│   └── Combine results
├── Output: LoadResult with manifest generation status
└── Log: Manifest generation progress and results
```

### Step 3: File Upload
```
UploadFilesCommand
├── Input: IEnumerable<SourceFile>, OutputPath
├── Handler: UploadFilesCommandHandler
│   ├── Validate files (size, type, accessibility)
│   ├── For each file:
│   │   ├── IOsduService.UploadFileAsync()
│   │   ├── HTTP POST to /api/file/v2/files/uploadURL
│   │   ├── Get signed URL for upload
│   │   ├── Upload file to storage
│   │   └── Register file metadata in OSDU
│   ├── Track success/failure counts
│   └── Generate upload summary
├── Output: LoadResult with upload statistics
└── Log: Upload progress and results per file
```

### Step 4: Workflow Submission
```
SubmitWorkflowCommand
├── Input: WorkflowRequest, WorkflowType, Description
├── Handler: SubmitWorkflowCommandHandler
│   ├── Build execution context:
│   │   ├── AppKey: "osdu-data-load-tno"
│   │   └── data-partition-id: from configuration
│   ├── IOsduService.SubmitWorkflowAsync()
│   ├── HTTP POST to /api/workflow/v1/workflow
│   └── Return workflow execution status
├── Output: LoadResult with workflow submission status
└── Log: Workflow submission status and ID
```

## Service Layer (OSDU.DataLoad.Infrastructure)

### IOsduService Implementation
```
OsduService : IOsduService
├── Constructor(IOsduClient, ILogger, IOptions<OsduOptions>)
├── AuthenticateAsync()
│   ├── Get OAuth token from Azure AD
│   └── Set authorization headers
├── CreateLegalTagAsync(legalTagName)
│   ├── Build legal tag payload
│   ├── POST /api/legal/v1/legaltags
│   └── Return creation result
├── UploadFileAsync(file, uploadPath)
│   ├── Generate file metadata
│   ├── Get upload URL from OSDU
│   ├── Upload file to storage
│   ├── Register file in OSDU
│   └── Return FileUploadResult
├── SubmitWorkflowAsync(workflowRequest, workflowType)
│   ├── Build workflow payload
│   ├── POST /api/workflow/v1/workflow
│   └── Return workflow execution result
└── Error handling and retry logic
```

### PathConfiguration
```
PathConfiguration
├── BaseDataPath: Root directory for all data
├── InputPath: Source data directory
├── ManifestsPath: Generated manifests directory
├── DatasetsPath: Files to upload directory
├── OutputPath: Processing outputs directory
├── WorkProductManifestsPath: Work product manifests
└── NonWorkProductManifestsPath: Reference data manifests
```

## Data Flow and Transformations

### CSV to JSON Manifest Generation
```
CSV Files (TNO Data)
├── Python Script: csv_to_json.py
│   ├── Read CSV with headers
│   ├── Apply OSDU schema templates
│   ├── Transform data fields:
│   │   ├── Date formatting
│   │   ├── Coordinate transformations
│   │   ├── Unit conversions
│   │   └── OSDU-specific field mapping
│   └── Generate JSON manifests
├── JSON Manifests (OSDU Format)
│   ├── Work Product Manifests
│   │   ├── Well data
│   │   ├── Wellbore data
│   │   ├── Log data
│   │   └── Marker data
│   └── Non-Work Product Manifests
│       ├── Reference data
│       ├── Master data
│       └── Lookup tables
└── Validation and error reporting
```

### File Processing Pipeline
```
Source Files
├── File Discovery (datasets/ directory)
├── File Validation
│   ├── Size checks (prevent large file issues)
│   ├── Type validation (exclude .json, .log, .txt)
│   ├── Accessibility checks
│   └── Duplicate detection
├── File Upload to OSDU
│   ├── Generate file metadata
│   ├── Get signed upload URL
│   ├── Stream upload to storage
│   └── Register in OSDU catalog
└── Upload Result Tracking
```

## Error Handling and Resilience

### Exception Hierarchy
```
Application Exceptions
├── Configuration Errors
│   ├── Missing environment variables
│   ├── Invalid OSDU endpoints
│   └── Authentication failures
├── Data Processing Errors
│   ├── CSV parsing failures
│   ├── Schema validation errors
│   └── File access issues
├── Service Communication Errors
│   ├── HTTP timeout errors
│   ├── Network connectivity issues
│   ├── OSDU API errors
│   └── Authentication token expiry
└── System Errors
    ├── Out of memory
    ├── Disk space issues
    └── Permission errors
```

### Retry and Recovery
```
Retry Strategies
├── HTTP Client Retries
│   ├── Exponential backoff
│   ├── Circuit breaker pattern
│   └── Maximum retry attempts
├── File Upload Retries
│   ├── Resume partial uploads
│   ├── Retry on network errors
│   └── Skip successfully uploaded files
└── Authentication Refresh
    ├── Token expiry detection
    ├── Automatic re-authentication
    └── Request replay
```

## Configuration Management

### Environment Variables
```
OSDU Configuration
├── OSDU_BaseUrl: Platform API endpoint
├── OSDU_TenantId: Azure AD tenant
├── OSDU_ClientId: Application registration
├── OSDU_DataPartition: Data partition name
├── OSDU_LegalTag: Legal tag for data classification
├── OSDU_AclViewer: Read access group
├── OSDU_AclOwner: Write access group
└── OSDU_UserEmail: User for group assignment
```

### PathConfiguration Resolution
```
Path Resolution
├── BaseDataPath:
│   ├── Environment: OSDU_DATA_PATH
│   ├── Default: ~/osdu-data/tno
│   └── Container: /data
├── Relative Paths:
│   ├── manifests/ (generated manifests)
│   ├── datasets/ (files to upload)
│   ├── TNO/ (source data)
│   └── output/ (processing results)
└── Cross-platform compatibility
```

## Performance Considerations

### Scalability Factors
```
Performance Optimization
├── Parallel File Processing
│   ├── Concurrent uploads (limited by OSDU rate limits)
│   ├── Async/await patterns
│   └── Memory-efficient streaming
├── Batch Processing
│   ├── Manifest generation in batches
│   ├── File upload queuing
│   └── Progress reporting
├── Memory Management
│   ├── Streaming large files
│   ├── Dispose patterns
│   └── Garbage collection optimization
└── Network Optimization
    ├── Connection pooling
    ├── Keep-alive connections
    └── Compression support
```

### Monitoring and Observability
```
Telemetry and Logging
├── Structured Logging
│   ├── Request/response correlation
│   ├── Performance metrics
│   ├── Error tracking
│   └── Business metrics
├── Health Checks
│   ├── OSDU connectivity
│   ├── Authentication status
│   ├── File system access
│   └── Python script availability
└── Container Metrics
    ├── CPU and memory usage
    ├── Network I/O
    ├── Disk I/O
    └── Exit codes
```

This technical documentation provides the complete picture of how the OSDU Data Load TNO application processes data from initial CSV files through to successful upload and workflow submission in the OSDU platform.
