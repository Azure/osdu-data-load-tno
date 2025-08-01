using OSDU.DataLoad.Domain.Entities;

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
        TnoDataType.WellMarkers,
        TnoDataType.WellboreTrajectories,
        TnoDataType.WellLogs,
        TnoDataType.Documents,
    };

    /// <summary>
    /// Gets the subdirectory mapping for each data type (matches Python solution structure)
    /// </summary>
    public static readonly Dictionary<TnoDataType, string> ManifestDirectories = new()
    {
        { TnoDataType.ReferenceData, "manifests/reference-manifests" },
        { TnoDataType.MiscMasterData, "manifests/misc-master-data-manifests" },
        { TnoDataType.Wells, "manifests/master-well-data-manifests" },
        { TnoDataType.Wellbores, "manifests/master-wellbore-data-manifests" },

        { TnoDataType.Documents, "manifests/documents-manifests" },
        { TnoDataType.WellLogs, "manifests/well-logs-manifests" },
        { TnoDataType.WellMarkers, "manifests/well-markers-manifests" },
        { TnoDataType.WellboreTrajectories, "manifests/wellbore-trajectories-manifests" }
    };
}

/// <summary>
/// Configuration for dataset directory processing and output file mapping
/// </summary>
public static class DatasetConfiguration
{
    /// <summary>
    /// Maps dataset directory names to their corresponding output file names for tracking uploads
    /// </summary>
    public static readonly Dictionary<string, string> DatasetDirectoryToOutputFile = new()
    {
        { "datasets/documents", "loaded-documents-datasets.json" },
        { "datasets/well-logs", "loaded-welllogs-datasets.json" },
        { "datasets/markers", "loaded-marker-datasets.json" },
        { "datasets/trajectories", "loaded-trajectories-datasets.json" }
    };

    /// <summary>
    /// Gets the output file name for a given dataset directory
    /// </summary>
    /// <param name="datasetDirectory">The dataset directory path (relative)</param>
    /// <returns>The output file name, or null if not found</returns>
    public static string? GetOutputFileName(string datasetDirectory)
    {
        // Normalize the directory path for comparison
        var normalizedPath = datasetDirectory.Replace('\\', '/').ToLowerInvariant();
        
        // Try exact match first
        if (DatasetDirectoryToOutputFile.TryGetValue(normalizedPath, out var outputFile))
        {
            return outputFile;
        }

        // Try to find by directory name (last part of path)
        var directoryName = Path.GetFileName(normalizedPath);
        foreach (var (key, value) in DatasetDirectoryToOutputFile)
        {
            if (key.EndsWith($"/{directoryName}", StringComparison.OrdinalIgnoreCase))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>
    /// Gets all configured dataset directory names
    /// </summary>
    public static IEnumerable<string> GetDatasetDirectories()
    {
        return DatasetDirectoryToOutputFile.Keys;
    }

    /// <summary>
    /// Gets the output file mappings for work product data types
    /// </summary>
    /// <param name="outputPath">The base output directory path</param>
    /// <returns>Dictionary mapping TnoDataType to output file paths</returns>
    //public static Dictionary<TnoDataType, string> GetWorkProductOutputFileMappings(string outputPath)
    //{
    //    return new Dictionary<TnoDataType, string>
    //    {
    //        { TnoDataType.Documents, Path.Combine(outputPath, DatasetDirectoryToOutputFile["datasets/documents"]) },
    //        { TnoDataType.WellLogs, Path.Combine(outputPath, DatasetDirectoryToOutputFile["datasets/well-logs"]) },
    //        { TnoDataType.WellMarkers, Path.Combine(outputPath, DatasetDirectoryToOutputFile["datasets/markers"]) },
    //        { TnoDataType.WellboreTrajectories, Path.Combine(outputPath, DatasetDirectoryToOutputFile["datasets/trajectories"]) }
    //    };
    //}
}

/// <summary>
/// Configuration for manifest generation processing
/// </summary>
public static class ManifestGenerationConfiguration
{
    /// <summary>
    /// Configuration for non-work product manifest generation
    /// </summary>
    public static readonly ManifestGenerationConfig[] NonWorkProductManifestConfigs = new[]
    {
        new ManifestGenerationConfig
        {
            Type = "reference_data",
            MappingFile = "tno_ref_data_template_mapping.json",
            DataDir = "reference-data",
            OutputDir = "reference-manifests",
            GroupFile = true
        },
        new ManifestGenerationConfig
        {
            Type = "master_data",
            MappingFile = "tno_misc_master_data_template_mapping.json",
            DataDir = "master-data/Misc_master_data",
            OutputDir = "misc-master-data-manifests",
            GroupFile = true
        },
        new ManifestGenerationConfig
        {
            Type = "master_data",
            MappingFile = "tno_well_data_template_mapping.json",
            DataDir = "master-data/Well",
            OutputDir = "master-well-data-manifests",
            GroupFile = false
        },
        new ManifestGenerationConfig
        {
            Type = "master_data",
            MappingFile = "tno_wellbore_data_template_mapping.json",
            DataDir = "master-data/Wellbore",
            OutputDir = "master-wellbore-data-manifests",
            GroupFile = false
        }
    };

    public static readonly ManifestGenerationConfig[] WorkProductManifestConfigs = new[]
    {
        new ManifestGenerationConfig
        {
            Type = "WellMarkers",
            MappingFile = "loaded-marker-datasets.json",
            DataDir = "TNO/provided/work-products/markers",
            OutputDir = "well-markers-manifests",
            GroupFile = false
        },
        new ManifestGenerationConfig
        {
            Type = "WellboreTrajectories",
            MappingFile = "loaded-trajectories-datasets.json",
            DataDir = "TNO/provided/work-products/trajectories",
            OutputDir = "wellbore-trajectories-manifests",
            GroupFile = false
        },
        new ManifestGenerationConfig
        {
            Type = "WellLogs",
            MappingFile = "loaded-welllogs-datasets.json",
            DataDir = "TNO/provided/work-products/well logs",
            OutputDir = "well-logs-manifests",
            GroupFile = false
        },
        new ManifestGenerationConfig
        {
            Type = "Documents",
            MappingFile = "loaded-documents-datasets.json",
            DataDir = "TNO/provided/work-products/documents",
            OutputDir = "documents-manifests",
            GroupFile = false
        }
    };
}

/// <summary>
/// Configuration for individual manifest generation
/// </summary>
public class ManifestGenerationConfig
{
    public string Type { get; init; } = string.Empty;
    public string MappingFile { get; init; } = string.Empty;
    public string DataDir { get; init; } = string.Empty;
    public string OutputDir { get; init; } = string.Empty;
    public bool GroupFile { get; init; }
}
