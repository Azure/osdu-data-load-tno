namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Represents the result of a data loading operation
/// </summary>
public class LoadResult
{
    public bool IsSuccess { get; set; }
    public string? Message { get; set; }
    public string? ErrorDetails { get; set; }
    public int ProcessedRecords { get; set; }
    public int SuccessfulRecords { get; set; }
    public int FailedRecords { get; set; }
    public TimeSpan Duration { get; set; }
    public string? RunId { get; set; }
}

/// <summary>
/// Represents configuration for OSDU connection
/// </summary>
public class OsduConfiguration
{
    public string BaseUrl { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public string TenantId { get; set; } = string.Empty;
    public string DataPartition { get; set; } = string.Empty;
    public string LegalTag { get; set; } = string.Empty;
    public string AclViewer { get; set; } = string.Empty;
    public string AclOwner { get; set; } = string.Empty;
    public string UserEmail { get; set; } = string.Empty;
    public int RetryCount { get; set; } = 3;
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(2);
    public int BatchSize { get; set; } = 500;
    public int RequestTimeoutMs { get; set; } = 30000;
    public int FileUploadTimeoutMs { get; set; } = 300000; // 5 minutes for file uploads
    public string TestDataUrl { get; set; } = string.Empty;
    
    /// <summary>
    /// Gets the OAuth2 scope for OSDU API access using the ClientId
    /// </summary>
    public string AuthScope => $"{ClientId}/.default";
}

/// <summary>
/// Represents a data record to be loaded into OSDU
/// </summary>
public class DataRecord
{
    public string Id { get; init; } = string.Empty;
    public string Kind { get; init; } = string.Empty;
    public Dictionary<string, object> Data { get; init; } = new();
    public Dictionary<string, object> Legal { get; init; } = new();
    public Dictionary<string, object> Acl { get; init; } = new();
    //public Dictionary<string, object> Meta { get; init; } = new();
    public string? Ancestry { get; init; }
    public Dictionary<string, string> Tags { get; init; } = new();
}

/// <summary>
/// Represents the source file information for data loading
/// </summary>
public class SourceFile
{
    public string FilePath { get; init; } = string.Empty;
    public string FileName { get; init; } = string.Empty;
    public string FileType { get; init; } = string.Empty;
    public long Size { get; init; }
    public DateTime LastModified { get; init; }
}

/// <summary>
/// Represents the manifest for data loading operations
/// </summary>
public class LoadingManifest
{
    public string Version { get; init; } = string.Empty;
    public string Kind { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public SourceFile[] SourceFiles { get; init; } = Array.Empty<SourceFile>();
    public Dictionary<string, object> Configuration { get; init; } = new();
    public string[] Dependencies { get; init; } = Array.Empty<string>();
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
}

/// <summary>
/// Represents validation result for data records
/// </summary>
public class ValidationResult
{
    public bool IsValid { get; init; }
    public string[] Errors { get; init; } = Array.Empty<string>();
    public string[] Warnings { get; init; } = Array.Empty<string>();
    public string? RecordId { get; init; }
}

/// <summary>
/// Enumeration of supported TNO data types
/// </summary>
public enum TnoDataType
{
    Wells,
    Wellbores,
    WellboreTrajectories,
    WellMarkers,
    WellboreMarkers,
    WellLogs,
    ReferenceData,
    Horizons,
    Formations,
    WellCompletions,
    Documents,
    MiscMasterData,
    WorkProducts
}

/// <summary>
/// Represents the data loading phases and their order
/// </summary>
public static class DataLoadingOrder
{
    /// <summary>
    /// Gets the ordered list of data types to load for a complete TNO data load operation
    /// Based on dependencies and OSDU requirements
    /// </summary>
    public static readonly TnoDataType[] LoadingSequence = new[]
    {
        // Phase 1: Reference Data (must be loaded first)
        TnoDataType.ReferenceData,
        
        // Phase 2: Master Data (depends on reference data)
        TnoDataType.MiscMasterData,
        TnoDataType.Wells,
        TnoDataType.Wellbores,
        
        // Phase 3: File Data (depends on master data)
        TnoDataType.Documents,
        TnoDataType.WellLogs,
        TnoDataType.WellMarkers,
        TnoDataType.WellboreTrajectories,
        
        // Phase 4: Work Products (depends on file data)
        TnoDataType.WorkProducts
    };

    /// <summary>
    /// Gets the subdirectory mapping for each data type (matches Python solution structure)
    /// </summary>
    public static readonly Dictionary<TnoDataType, string> DirectoryMapping = new()
    {
        { TnoDataType.ReferenceData, "manifests/reference-manifests" },
        { TnoDataType.MiscMasterData, "manifests/misc-master-data-manifests" },
        { TnoDataType.Wells, "manifests/master-well-data-manifests" },
        { TnoDataType.Wellbores, "manifests/master-wellbore-data-manifests" },
        { TnoDataType.Documents, "TNO/provided/TNO/work-products/documents" },
        { TnoDataType.WellLogs, "TNO/provided/TNO/work-products/well logs" },
        { TnoDataType.WellMarkers, "TNO/provided/TNO/work-products/markers" },
        { TnoDataType.WellboreTrajectories, "TNO/provided/TNO/work-products/trajectories" },
        { TnoDataType.WorkProducts, "TNO/provided/TNO/work-products" }
    };
}
