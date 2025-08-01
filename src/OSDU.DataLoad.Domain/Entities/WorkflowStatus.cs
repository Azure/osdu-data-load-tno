namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Represents the status of an OSDU workflow run
/// </summary>
public class WorkflowStatus
{
    /// <summary>
    /// The workflow identifier
    /// </summary>
    public string WorkflowId { get; set; } = string.Empty;

    /// <summary>
    /// The unique run identifier
    /// </summary>
    public string RunId { get; set; } = string.Empty;

    /// <summary>
    /// The start timestamp in milliseconds
    /// </summary>
    public long StartTimeStamp { get; set; }

    /// <summary>
    /// The end timestamp in milliseconds (if finished)
    /// </summary>
    public long? EndTimeStamp { get; set; }

    /// <summary>
    /// The current status of the workflow (running, finished, failed)
    /// </summary>
    public string Status { get; set; } = string.Empty;

    /// <summary>
    /// The user who submitted the workflow
    /// </summary>
    public string SubmittedBy { get; set; } = string.Empty;

    /// <summary>
    /// Indicates if the workflow is still running
    /// </summary>
    public bool IsRunning => Status.Equals("running", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Indicates if the workflow finished successfully
    /// </summary>
    public bool IsFinished => Status.Equals("finished", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Indicates if the workflow failed
    /// </summary>
    public bool IsFailed => Status.Equals("failed", StringComparison.OrdinalIgnoreCase);
}
