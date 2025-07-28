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
}
