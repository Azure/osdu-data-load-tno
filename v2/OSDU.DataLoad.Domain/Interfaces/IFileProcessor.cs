using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for file operations
/// </summary>
public interface IFileProcessor
{
    /// <summary>
    /// Discovers source files in a directory
    /// </summary>
    Task<SourceFile[]> DiscoverFilesAsync(string directoryPath, TnoDataType dataType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads and parses a source file
    /// </summary>
    Task<T> ReadFileAsync<T>(SourceFile sourceFile, CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates file format and accessibility
    /// </summary>
    Task<ValidationResult> ValidateFileAsync(SourceFile sourceFile, CancellationToken cancellationToken = default);
}
