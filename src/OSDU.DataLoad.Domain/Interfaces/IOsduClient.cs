using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Interface for OSDU API client operations
/// </summary>
public interface IOsduClient
{
    /// <summary>
    /// Authenticates with the OSDU platform
    /// </summary>
    Task<bool> AuthenticateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Uploads data records to OSDU
    /// </summary>
    Task<LoadResult> UploadRecordsAsync(IEnumerable<DataRecord> records, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if a record exists in OSDU
    /// </summary>
    Task<bool> RecordExistsAsync(string recordId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets schema information for a specific kind
    /// </summary>
    Task<string> GetSchemaAsync(string kind, CancellationToken cancellationToken = default);

    /// <summary>
    /// Validates records against OSDU schema
    /// </summary>
    Task<ValidationResult[]> ValidateRecordsAsync(IEnumerable<DataRecord> records, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uploads a file to OSDU following the complete workflow: get upload URL, upload to blob, post metadata, get version
    /// </summary>
    Task<FileUploadResult> UploadFileAsync(string filePath, string fileName, string description, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a user to the OSDU data lake operations group (users.datalake.ops@{dataPartition}.dataservices.energy)
    /// </summary>
    Task<bool> AddUserToOpsGroupAsync(string dataPartition, string userEmail, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a legal tag in OSDU
    /// </summary>
    Task<bool> CreateLegalTagAsync(string legalTagName, CancellationToken cancellationToken = default);
}
