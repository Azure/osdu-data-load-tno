using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for data transformation operations
/// </summary>
public interface IDataTransformer
{
    /// <summary>
    /// Transforms source data to OSDU format
    /// </summary>
    Task<DataRecord[]> TransformAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates source data format
    /// </summary>
    Task<ValidationResult> ValidateSourceAsync(SourceFile sourceFile, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets supported file extensions for the data type
    /// </summary>
    string[] GetSupportedExtensions(TnoDataType dataType);
}
