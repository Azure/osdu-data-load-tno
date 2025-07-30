using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for manifest generation operations
/// </summary>
public interface IManifestGenerator
{
    /// <summary>
    /// Generates a loading manifest for source files
    /// </summary>
    Task<LoadingManifest> GenerateManifestAsync(SourceFile[] sourceFiles, TnoDataType dataType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates a loading manifest
    /// </summary>
    Task<ValidationResult> ValidateManifestAsync(LoadingManifest manifest, CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves manifest to file
    /// </summary>
    Task SaveManifestAsync(LoadingManifest manifest, string filePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Generates OSDU manifests from CSV data using the specified mapping configuration
    /// </summary>
    /// <param name="mappingFilePath">Path to the JSON mapping configuration file</param>
    /// <param name="templateType">Type of template (e.g., "reference_data", "master_data")</param>
    /// <param name="dataDirectory">Directory containing CSV data files</param>
    /// <param name="outputDirectory">Directory where generated manifests will be written</param>
    /// <param name="homeDirectory">Base directory containing templates and data</param>
    /// <param name="dataPartition">OSDU data partition name</param>
    /// <param name="groupFile">Whether to group all manifests into a single file</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>True if successful, false otherwise</returns>
    Task<bool> GenerateManifestsFromCsvAsync(
        string mappingFilePath,
        string templateType,
        string dataDirectory,
        string outputDirectory,
        string homeDirectory,
        string dataPartition,
        bool groupFile = false,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enhanced manifest generation with full Python logic compatibility
    /// Equivalent to Python's create_manifest_from_csv with advanced options
    /// </summary>
    /// <param name="csvFilePath">Path to CSV data file</param>
    /// <param name="templatePath">Path to JSON template file</param>
    /// <param name="outputDirectory">Output directory for generated manifests</param>
    /// <param name="options">Advanced processing options</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task GenerateManifestsFromCsvWithOptionsAsync(
        string csvFilePath,
        string templatePath,
        string outputDirectory,
        TemplateProcessingOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Set progress reporter for detailed progress tracking
    /// </summary>
    /// <param name="progressReporter">Progress reporter implementation</param>
    void SetProgressReporter(IManifestProgressReporter progressReporter);
}
