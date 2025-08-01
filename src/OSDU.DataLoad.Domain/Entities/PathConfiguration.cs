namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Centralized configuration for all file and directory paths used throughout the application
/// </summary>
public class PathConfiguration
{
    /// <summary>
    /// Base data directory path
    /// </summary>
    public string BaseDataPath { get; set; } = string.Empty;
    
    /// <summary>
    /// Input CSV files directory
    /// </summary>
    public string InputPath => Path.Combine(BaseDataPath, "TNO");
    
    /// <summary>
    /// Generated manifests output directory
    /// </summary>
    public string ManifestsPath => Path.Combine(BaseDataPath, "manifests");
    
    /// <summary>
    /// Dataset files directory
    /// </summary>
    public string DatasetsPath => Path.Combine(BaseDataPath, "datasets");
    
    /// <summary>
    /// Work product manifests directory
    /// </summary>
    public string WorkProductManifestsPath => Path.Combine(ManifestsPath, "work-products");
    
    /// <summary>
    /// Non-work product manifests directory
    /// </summary>
    public string NonWorkProductManifestsPath => Path.Combine(ManifestsPath, "non-work-products");
    
    /// <summary>
    /// File location mappings output directory
    /// </summary>
    public string FileLocationMappingsPath => Path.Combine(BaseDataPath, "file-location-mappings");
    
    /// <summary>
    /// Python script directory for manifest generation
    /// </summary>
    public string PythonScriptsPath => Path.Combine(BaseDataPath, "..", "..", "generate-manifest-scripts");
    
    /// <summary>
    /// Temporary files directory
    /// </summary>
    public string TempPath => Path.Combine(BaseDataPath, "temp");
}
