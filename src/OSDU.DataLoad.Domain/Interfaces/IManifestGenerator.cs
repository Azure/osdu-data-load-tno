using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for manifest generation operations
/// </summary>
public interface IManifestGenerator
{
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
            string aclViewer,
            string aclOwner,
            string legalTag = null,
            bool groupFile = false,
            CancellationToken cancellationToken = default);
}
