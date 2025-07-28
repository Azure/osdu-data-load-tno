namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Represents progress information for data loading operations
/// </summary>
public class LoadProgress
{
    public int TotalRecords { get; init; }
    public int ProcessedRecords { get; init; }
    public int SuccessfulRecords { get; init; }
    public int FailedRecords { get; init; }
    public double PercentageComplete => TotalRecords > 0 ? (double)ProcessedRecords / TotalRecords * 100 : 0;
    public TimeSpan ElapsedTime { get; init; }
    public TimeSpan? EstimatedTimeRemaining { get; init; }
    public string? CurrentOperation { get; init; }
}
