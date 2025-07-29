# Data Loading Process

This document explains the complete data loading process for the OSDU Data Load TNO application.

## Process Overview

The data loading process follows an orchestrated workflow that automatically loads all TNO data types in the correct dependency order:

```mermaid
graph TD
    A[CLI Input] --> B["load --source &lt;path&gt;"]
    
    B --> C[LoadAllDataCommand]
    C --> P[Prepare Stage]
    P --> P1{UserEmail Configured?}
    P1 -->|Yes| P2[AddUserToOpsGroup]
    P1 -->|No| P3[Skip User Setup]
    P2 --> P4[CreateLegalTag]
    P3 --> P4
    P4 --> D{For Each Data Type in Order}
    
    D --> E1[Reference Data]
    E1 --> E2[Misc Master Data]
    E2 --> E3[Wells]
    E3 --> E4[Wellbores]
    E4 --> E5[Documents]
    E5 --> E6[Well Logs]
    E6 --> E7[Well Markers]
    E7 --> E8[Wellbore Trajectories]
    E8 --> E9[Work Products]
    
    E1 --> F[LoadDataCommand per Type]
    E2 --> F
    E3 --> F
    E4 --> F
    E5 --> F
    E6 --> F
    E7 --> F
    E8 --> F
    E9 --> F
    
    F --> G[DiscoverFilesQuery]
    G --> H[GenerateManifestCommand]
    H --> I[LoadFromManifestCommand]
    I --> J[TransformDataCommand]
    J --> K[UploadRecordsCommand]
    K --> L[Aggregate Results]
    L --> M[Display Complete Summary]
    
    style C fill:#e1f5fe
    style P fill:#fff3e0
    style P2 fill:#e8f5e8
    style P4 fill:#e8f5e8
    style F fill:#ffebee
    style J fill:#e8f5e8
```

## Orchestrated Loading Order

The solution automatically processes data in this order:

### 0. **Prepare Stage** → Environment setup and prerequisites
   - **User Authorization**: Adds configured user to `users.datalake.ops@{dataPartition}.dataservices.energy` group
   - **Legal Tag Creation**: Creates the specified legal tag with standard compliance properties
   
### 1. **Reference Data** → Foundation data required by all other types
2. **Misc Master Data** → Additional master data dependencies
3. **Wells** → Well master data
4. **Wellbores** → Wellbore master data (depends on wells)
5. **Documents** → Document files
6. **Well Logs** → Well log files
7. **Well Markers** → Marker data files
8. **Wellbore Trajectories** → Trajectory data files
9. **Work Products** → Final work product data

## Detailed Process Flow

### Complete Command Orchestration Overview
```mermaid
sequenceDiagram
    participant CLI as CLI Application
    participant LoadAll as LoadAllDataHandler
    participant Prepare as Prepare Stage
    participant Load as LoadFromManifestHandler
    participant Discover as DiscoverFilesQuery
    participant Upload as UploadFilesCommand
    participant Transform as TransformDataCommand
    participant Records as UploadRecordsCommand
    participant OSDU as OSDU Platform APIs

    CLI->>LoadAll: LoadAllDataCommand(SourcePath)
    
    Note over LoadAll,Prepare: Prepare Stage - Environment Setup
    LoadAll->>Prepare: AddUserToOpsGroupCommand
    Prepare->>OSDU: POST /api/entitlements/v2/groups/{group}/members
    OSDU-->>Prepare: User added (or already exists)
    LoadAll->>Prepare: CreateLegalTagCommand
    Prepare->>OSDU: POST /api/legal/v1/legaltags
    OSDU-->>Prepare: Legal tag created (or already exists)
    
    Note over LoadAll,Load: Data Loading Phase - For Each Data Type
    LoadAll->>Load: LoadFromManifestCommand(DataType)
    Load->>Discover: DiscoverFilesQuery(DirectoryPath)
    Discover-->>Load: SourceFile[]
    
    alt Files require upload (RequiresFileUpload=true)
        Load->>Upload: UploadFilesCommand(SourceFiles)
        Upload->>OSDU: File upload workflow
        Note right of OSDU: /api/file/v2/files/uploadURL<br/>/api/file/v2/files/metadata<br/>Azure Blob Storage
        OSDU-->>Upload: File upload results
        Upload-->>Load: File upload complete
    end
    
    Load->>Transform: TransformDataCommand(SourceFile, DataType)
    Transform-->>Load: DataRecord[]
    
    Load->>Records: UploadRecordsCommand(DataRecords)
    Records->>OSDU: Batch upload workflow
    Note right of OSDU: /api/storage/v2/records<br/>/api/schema-service/v1/schema<br/>Max 500 records per batch
    OSDU-->>Records: Upload results
    Records-->>Load: Final results
    Load-->>LoadAll: LoadResult per data type
    LoadAll-->>CLI: Complete aggregated LoadResult
```

## Prepare Stage Details

The prepare stage ensures that the OSDU environment is properly configured before data loading begins. This stage includes user authorization setup and legal tag creation.

### User Authorization Setup

When `OSDU_USER_EMAIL` is configured, the application will:

1. **Check User Authorization**: Adds the specified user to the data partition operations group
2. **Group Pattern**: Uses the format `users.datalake.ops@{dataPartition}.dataservices.energy`
3. **Conflict Handling**: Gracefully handles 409 responses when user is already in the group
4. **Error Logging**: Provides detailed logging for authorization operations

**Configuration Example:**
```bash
export OSDU_USER_EMAIL="john.doe@example.com"
export OSDU_DATA_PARTITION="opendes"
```

This will add `john.doe@example.com` to the group `users.datalake.ops@opendes.dataservices.energy`.

### Legal Tag Creation

When `OSDU_LEGAL_TAG` is configured, the application will:

1. **Create Legal Tag**: Creates a legal tag with the specified name
2. **Default Properties**: Uses standard compliance settings:
   - Countries of Origin: [US, CA]
   - Contract ID: No Contract Related
   - Data Type: Public Domain Data
   - Export Classification: EAR99
   - Originator: TNO
   - Personal Data: No Personal Data

3. **Conflict Handling**: Gracefully handles 409 responses when legal tag already exists
4. **Validation**: Ensures legal tag is available for data record creation

**Configuration Example:**
```bash
export OSDU_LEGAL_TAG="tno-geological-data-public"
```

### Error Handling

The prepare stage implements robust error handling:

- **409 Conflicts**: Treated as success conditions (resource already exists)
- **API Failures**: Logged with detailed error information
- **Continuation**: Data loading continues even if prepare stage encounters non-critical errors
- **Validation**: Validates OSDU configuration before attempting operations

### 1. File Discovery Phase
```mermaid
sequenceDiagram
    participant CLI as CLI Application
    participant DFQ as DiscoverFilesQuery
    participant FP as FileProcessor
    participant FS as File System

    CLI->>DFQ: DirectoryPath + DataType
    DFQ->>FP: Discover files by type
    FP->>FS: Scan directory structure
    FS-->>FP: File list
    FP->>FP: Filter by extensions
    FP->>FP: Create SourceFile entities
    FP-->>DFQ: SourceFile[]
    DFQ-->>CLI: Discovered files
```

### 2. Data Transformation Phase
```mermaid
sequenceDiagram
    participant Handler as LoadFromManifestHandler
    participant UploadFiles as UploadFilesCommandHandler
    participant Transform as TransformDataCommandHandler
    participant UploadRecords as UploadRecordsCommandHandler
    participant DT as DataTransformer
    participant Schema as Schema Service
    participant OSDU as OSDU APIs

    Handler->>Handler: Check RequiresFileUpload flag
    
    alt RequiresFileUpload = true
        Handler->>UploadFiles: UploadFilesCommand
        UploadFiles->>OSDU: File upload workflow
        OSDU-->>UploadFiles: File upload results
        UploadFiles-->>Handler: Upload complete
    end
    
    Handler->>Transform: TransformDataCommand
    Transform->>DT: Transform request (SourceFile + DataType)
    DT->>DT: Parse file format (CSV/JSON/Excel)
    DT->>DT: Map TNO fields to OSDU schema
    DT->>DT: Generate OSDU record structure
    
    opt Schema validation enabled
        DT->>Schema: GET /schema/{kind}
        Schema-->>DT: Schema definition
        DT->>DT: Validate against schema
    end
    
    DT-->>Transform: DataRecord[]
    Transform-->>Handler: Transformed records
    
    Handler->>UploadRecords: UploadRecordsCommand
    UploadRecords->>OSDU: Batch upload (≤500 records)
    OSDU-->>UploadRecords: Upload results
    UploadRecords-->>Handler: Final results
```

### 3. Upload Phase
```mermaid
sequenceDiagram
    participant Handler as UploadRecordsHandler
    participant Client as OsduHttpClient
    participant Auth as Azure Identity
    participant File as File API (/api/file/v2)
    participant Blob as Azure Blob Storage
    participant Storage as Storage API (/api/storage/v2)
    participant Schema as Schema Service (/api/schema-service/v1)

    Handler->>Client: Upload records + files
    Client->>Auth: Get access token
    Auth-->>Client: Bearer token
    
    alt Records require schema validation
        Client->>Schema: GET /schema/{kind}
        Schema-->>Client: Schema definition
    end
    
    alt RequiresFileUpload = true
        Note over Client,Blob: File Upload Workflow (4 steps)
        Client->>File: GET /files/uploadURL
        File-->>Client: Signed URL + FileSource ID
        Client->>Blob: PUT file to signed URL
        Blob-->>Client: Upload confirmation
        Client->>File: POST /files/metadata
        File-->>Client: File ID + metadata
        Client->>Storage: GET /records/{fileId}/versions
        Storage-->>Client: Record version info
    end
    
    Note over Client,Storage: Record Upload with Batching
    Client->>Storage: PUT /records (batch ≤500)
    Storage->>Storage: Validate + store records
    Storage-->>Client: Upload results
    Client-->>Handler: LoadResult + FileResults
```

### 4. Intelligent Batching
```mermaid
sequenceDiagram
    participant Handler as UploadRecordsHandler
    participant Client as OsduHttpClient
    participant Storage as Storage API (/api/storage/v2)

    Handler->>Client: Upload 929 records
    Note over Client: Split into batches (max 500 per batch)
    
    loop For each batch
        Client->>Client: Create batch (≤500 records)
        Client->>Storage: PUT /records
        Note right of Storage: OSDU limit: max 500 records
        Storage->>Storage: Validate + store batch
        Storage-->>Client: Batch result
    end
    
    Note over Client: Example with 929 records:
    Client->>Storage: PUT /records (Batch 1: 500 records)
    Storage-->>Client: Success: 500 uploaded
    
    Client->>Storage: PUT /records (Batch 2: 429 records)
    Storage-->>Client: Success: 429 uploaded
    
    Client->>Client: Aggregate all batch results
    Client-->>Handler: Final Result: 929 total uploaded
```

## File Upload Process

The C# implementation includes a complete **4-step file upload workflow** that matches the Python `load_single_file()` function:

1. **Get Upload URL**: Request signed URL from OSDU File API (`/files/uploadURL`)
2. **Upload to Blob**: Upload file to Azure Blob Storage using signed URL
3. **Post Metadata**: Submit file metadata to OSDU (`/files/metadata`)
4. **Get Version**: Retrieve record version from Storage API (`/records/{fileId}/versions`)

This ensures **complete integration** with OSDU platform file management and work product relationships.

## Data Types and Processing

| Data Type | Description | File Formats | Upload Method |
|-----------|-------------|--------------|---------------|
| `Wells` | Well master data | CSV, JSON, Excel | Records API |
| `Wellbores` | Wellbore information | CSV, JSON, Excel | Records API |
| `WellboreTrajectories` | Directional survey data | CSV, JSON | Records + Files API |
| `WellMarkers` | Geological markers | CSV, JSON | Records + Files API |
| `WellboreMarkers` | Wellbore-specific markers | CSV, JSON | Records + Files API |
| `WellLogs` | Log curve data | LAS, DLIS, CSV | Files API (4-step workflow) |
| `Documents` | Document files | PDF, DOC, XLS, etc. | Files API (4-step workflow) |
| `ReferenceData` | Lookup tables | CSV, JSON | Records API |
| `Horizons` | Geological horizons | CSV, JSON | Records API |
| `Formations` | Formation tops | CSV, JSON | Records API |
| `WellCompletions` | Completion data | CSV, JSON | Records API |
| `WorkProducts` | Work product metadata | JSON manifests | Records API with file references |

## Expected Directory Structure

The application expects the following directory structure (matches the Python solution's open-test-data format):

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
