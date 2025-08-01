using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Domain.Interfaces;

/// <summary>
/// Consolidated service interface for all OSDU operations
/// Provides high-level service operations and low-level HTTP client operations
/// </summary>
public interface IOsduService
{
    /// <summary>
    /// Creates a legal tag in OSDU (service-level operation)
    /// </summary>
    Task<LoadResult> CreateLegalTagAsync(string legalTagName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Uploads a single dataset file to OSDU
    /// </summary>
    Task<FileUploadResult> UploadFileAsync(SourceFile file, CancellationToken cancellationToken = default);

    /// <summary>
    /// Submits a workflow request to OSDU for manifest processing
    /// </summary>
    Task<LoadResult> SubmitWorkflowAsync(object workflowRequest, CancellationToken cancellationToken = default);

    Task<WorkflowStatus> GetWorkflowStatusAsync(string runId, CancellationToken cancellationToken = default);
}
