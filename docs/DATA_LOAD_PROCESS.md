# Data Loading Process

This document explains the complete data loading process for the OSDU Data Load TNO application.

## Process Overview

The data loading process follows an orchestrated workflow that automatically loads all TNO data types in the correct dependency order. The process consists of 6 main steps:

```mermaid
graph TD
    A[CLI Input] --> B["load --source path"]
    
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
    G --> H1{Has Files?}
    H1 -->|Yes| H2[UploadFilesCommand]
    H1 -->|No| H3[GenerateManifestsCommand]
    H2 --> H3
    H3 --> I[LoadFromManifestCommand]
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

## Data Loading Steps Explained

### Step 1: Download TNO Dataset Files
- Downloads official TNO test data from GitLab repository (~2.2GB)
- Extracts and organizes files into expected directory structure

### Step 2: Create Legal Tag
- Establishes required legal compliance tags for data governance
- Sets up legal framework required by OSDU platform
- **Handler**: `CreateLegalTagCommandHandler`

### Step 3: Upload Files to OSDU (4-step process)
- **Step 3a**: Request file upload URL from File API
- **Step 3b**: Upload file content to storage location
- **Step 3c**: Submit metadata to File Service
- **Step 3d**: Maintain registry of uploaded files with IDs and versions
- **Handler**: `UploadFilesCommandHandler`

### Step 4: Generate Non-Work Product Manifests
- Uses CSV templates to generate individual manifests for each data row
- Processes reference data, wells, wellbores, and related entities
- **Handler**: `GenerateManifestsCommandHandler`

### Step 5: Generate Work Product Manifests
- Iterates through uploaded files registry
- Retrieves JSON metadata from work product folders
- Updates manifests with legal tags, ACL permissions, and data partition IDs
- **Handler**: `GenerateWorkProductManifestCommandHandler`

### Step 6: Upload Manifests
- Submits all manifests to OSDU in correct dependency order:
  1. Reference Data (foundation lookup data)
  2. Misc Master Data (additional dependencies)
  3. Wells (well master data)
  4. Wellbores (depends on wells)
  5. Documents (document files)
  6. Well Logs (log files and data)
  7. Well Markers (geological markers)
  8. Wellbore Trajectories (directional surveys)
  9. Work Products (final metadata referencing uploaded files)
- Processes data types sequentially to maintain referential integrity
- **Handler**: `LoadFromManifestCommandHandler` → `UploadRecordsCommandHandler`
    style P fill:#fff3e0
    style P2 fill:#e8f5e8
    style P4 fill:#e8f5e8
    style F fill:#ffebee
    style J fill:#e8f5e8

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

## File Upload Process

The C# implementation includes a complete **4-step file upload workflow** that matches the Python `load_single_file()` function:

1. **Get Upload URL**: Request signed URL from OSDU File API (`/files/uploadURL`)
2. **Upload to Blob**: Upload file to Azure Blob Storage using signed URL
3. **Post Metadata**: Submit file metadata to OSDU (`/files/metadata`)
4. **Get Version**: Retrieve record version from Storage API (`/records/{fileId}/versions`)