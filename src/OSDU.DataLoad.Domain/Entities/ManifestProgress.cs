namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Detailed progress information for manifest generation operations
/// Provides comprehensive tracking with time estimates and performance metrics
/// </summary>
public class ManifestProgress
{
    public string OperationId { get; init; } = Guid.NewGuid().ToString();
    public DateTime StartTime { get; init; } = DateTime.UtcNow;
    public DateTime LastUpdateTime { get; init; } = DateTime.UtcNow;
    
    // Current phase information
    public ManifestGenerationPhase CurrentPhase { get; init; }
    public string CurrentPhaseDescription { get; init; } = string.Empty;
    public string CurrentFile { get; init; } = string.Empty;
    
    // Overall progress
    public int TotalCsvRows { get; init; }
    public int ProcessedCsvRows { get; init; }
    public int TotalManifestFiles { get; init; }
    public int GeneratedManifestFiles { get; init; }
    public int TotalBatches { get; init; }
    public int ProcessedBatches { get; init; }
    
    // Success/failure tracking
    public int SuccessfulRecords { get; init; }
    public int FailedRecords { get; init; }
    public List<string> ErrorMessages { get; init; } = new();
    
    // Performance metrics
    public TimeSpan ElapsedTime => DateTime.UtcNow - StartTime;
    public double RecordsPerSecond => ProcessedCsvRows > 0 && ElapsedTime.TotalSeconds > 0 
        ? ProcessedCsvRows / ElapsedTime.TotalSeconds : 0;
    
    // Progress percentages
    public double OverallPercentage => TotalCsvRows > 0 
        ? (double)ProcessedCsvRows / TotalCsvRows * 100 : 0;
    public double PhasePercentage { get; init; }
    
    // Time estimates
    public TimeSpan? EstimatedTimeRemaining => CalculateEstimatedTimeRemaining();
    public DateTime? EstimatedCompletionTime => EstimatedTimeRemaining.HasValue 
        ? DateTime.UtcNow.Add(EstimatedTimeRemaining.Value) : null;
    
    // Memory and I/O metrics
    public long MemoryUsageMB { get; init; }
    public int FileSystemOperations { get; init; }
    
    private TimeSpan? CalculateEstimatedTimeRemaining()
    {
        if (ProcessedCsvRows == 0 || TotalCsvRows == 0 || ProcessedCsvRows >= TotalCsvRows)
            return null;
            
        var remainingRows = TotalCsvRows - ProcessedCsvRows;
        var avgTimePerRow = ElapsedTime.TotalSeconds / ProcessedCsvRows;
        var estimatedSeconds = remainingRows * avgTimePerRow;
        
        return TimeSpan.FromSeconds(estimatedSeconds);
    }
}

/// <summary>
/// Phases of manifest generation operation
/// </summary>
public enum ManifestGenerationPhase
{
    Initializing,
    LoadingTemplate,
    LoadingCsvData,
    ValidatingData,
    ProcessingRecords,
    GeneratingBatches,
    WritingFiles,
    Finalizing,
    Completed,
    Failed
}

/// <summary>
/// Error information during manifest processing
/// </summary>
public class ManifestProcessingError
{
    public string ErrorId { get; init; } = Guid.NewGuid().ToString();
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    public string Message { get; init; } = string.Empty;
    public Exception? Exception { get; init; }
    public string? FileName { get; init; }
    public int? RowNumber { get; init; }
    public ManifestGenerationPhase Phase { get; init; }
    public Dictionary<string, object> Context { get; init; } = new();
}

/// <summary>
/// Final results of manifest generation operation
/// </summary>
public class ManifestGenerationResult
{
    public string OperationId { get; init; } = string.Empty;
    public bool IsSuccess { get; init; }
    public TimeSpan TotalDuration { get; init; }
    public DateTime CompletedAt { get; init; } = DateTime.UtcNow;
    
    // File and record statistics
    public int TotalCsvRows { get; init; }
    public int SuccessfulRecords { get; init; }
    public int FailedRecords { get; init; }
    public int GeneratedManifestFiles { get; init; }
    public int TotalBatches { get; init; }
    
    // Performance metrics
    public double RecordsPerSecond { get; init; }
    public long TotalMemoryUsed { get; init; }
    public int FileSystemOperations { get; init; }
    
    // Output information
    public List<string> GeneratedFiles { get; init; } = new();
    public List<ManifestProcessingError> Errors { get; init; } = new();
    public Dictionary<string, object> Metadata { get; init; } = new();
    
    // Summary message
    public string SummaryMessage => IsSuccess 
        ? $"Successfully generated {GeneratedManifestFiles} manifest files from {TotalCsvRows} CSV rows in {TotalDuration:mm\\:ss}"
        : $"Failed to generate manifests: {FailedRecords}/{TotalCsvRows} records failed";
}
