using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Diagnostics;

namespace OSDU.DataLoad.Infrastructure.Services;

/// <summary>
/// Console and logger-based progress reporter for manifest generation
/// Provides structured progress updates with performance metrics and time estimates
/// </summary>
public class ManifestProgressReporter : IManifestProgressReporter
{
    private readonly ILogger<ManifestProgressReporter> _logger;
    private readonly Stopwatch _stopwatch;
    private DateTime _lastProgressUpdate = DateTime.MinValue;
    private readonly TimeSpan _minUpdateInterval = TimeSpan.FromSeconds(2); // Avoid spam
    
    // Progress tracking state
    private ManifestProgress? _lastProgress;
    private readonly List<ManifestProcessingError> _errors = new();
    private int _totalFileSystemOps = 0;

    public ManifestProgressReporter(ILogger<ManifestProgressReporter> logger)
    {
        _logger = logger;
        _stopwatch = Stopwatch.StartNew();
    }

    public void Report(ManifestProgress progress)
    {
        _lastProgress = progress;
        
        // Throttle progress updates to avoid log spam
        var now = DateTime.UtcNow;
        if (now - _lastProgressUpdate < _minUpdateInterval && 
            progress.CurrentPhase != ManifestGenerationPhase.Completed &&
            progress.CurrentPhase != ManifestGenerationPhase.Failed)
        {
            return;
        }
        
        _lastProgressUpdate = now;
        LogProgress(progress);
    }

    public void ReportPhaseChange(ManifestGenerationPhase phase, string phaseDescription)
    {
        _logger.LogInformation("📋 Phase: {Phase} - {Description}", 
            GetPhaseIcon(phase) + phase.ToString(), phaseDescription);
        
        // Always log phase changes regardless of throttling
        _lastProgressUpdate = DateTime.MinValue;
    }

    public void ReportError(ManifestProcessingError error)
    {
        _errors.Add(error);
        
        var contextInfo = error.Context.Any() 
            ? " | Context: " + string.Join(", ", error.Context.Select(kvp => $"{kvp.Key}={kvp.Value}"))
            : "";
            
        if (error.RowNumber.HasValue)
        {
            _logger.LogWarning("⚠️ Row {RowNumber}: {Message}{Context}", 
                error.RowNumber, error.Message, contextInfo);
        }
        else if (!string.IsNullOrEmpty(error.FileName))
        {
            _logger.LogWarning("⚠️ File {FileName}: {Message}{Context}", 
                error.FileName, error.Message, contextInfo);
        }
        else
        {
            _logger.LogWarning("⚠️ {Message}{Context}", error.Message, contextInfo);
        }
    }

    public void ReportCompletion(ManifestGenerationResult finalResult)
    {
        var icon = finalResult.IsSuccess ? "✅" : "❌";
        
        _logger.LogInformation("");
        _logger.LogInformation("═══════════════════════════════════════════════════════════");
        _logger.LogInformation("{Icon} MANIFEST GENERATION COMPLETE", icon);
        _logger.LogInformation("═══════════════════════════════════════════════════════════");
        _logger.LogInformation("");
        
        // Overall results
        _logger.LogInformation("📊 Overall Results:");
        _logger.LogInformation("   Status: {Status}", finalResult.IsSuccess ? "SUCCESS" : "FAILED");
        _logger.LogInformation("   Duration: {Duration:mm\\:ss\\.fff}", finalResult.TotalDuration);
        _logger.LogInformation("   Operation ID: {OperationId}", finalResult.OperationId);
        _logger.LogInformation("");
        
        // Record statistics
        _logger.LogInformation("📈 Record Processing:");
        _logger.LogInformation("   Total CSV Rows: {TotalRows:N0}", finalResult.TotalCsvRows);
        _logger.LogInformation("   Successful Records: {Successful:N0}", finalResult.SuccessfulRecords);
        _logger.LogInformation("   Failed Records: {Failed:N0}", finalResult.FailedRecords);
        _logger.LogInformation("   Success Rate: {Rate:P1}", 
            finalResult.TotalCsvRows > 0 ? (double)finalResult.SuccessfulRecords / finalResult.TotalCsvRows : 0);
        _logger.LogInformation("");
        
        // File generation statistics
        _logger.LogInformation("📄 File Generation:");
        _logger.LogInformation("   Manifest Files Created: {Files:N0}", finalResult.GeneratedManifestFiles);
        _logger.LogInformation("   Total Batches: {Batches:N0}", finalResult.TotalBatches);
        if (finalResult.GeneratedFiles.Any())
        {
            _logger.LogInformation("   Output Files:");
            foreach (var file in finalResult.GeneratedFiles.Take(10)) // Limit to first 10
            {
                _logger.LogInformation("     • {FileName}", Path.GetFileName(file));
            }
            if (finalResult.GeneratedFiles.Count > 10)
            {
                _logger.LogInformation("     ... and {Count} more files", finalResult.GeneratedFiles.Count - 10);
            }
        }
        _logger.LogInformation("");
        
        // Performance metrics
        _logger.LogInformation("⚡ Performance Metrics:");
        _logger.LogInformation("   Records/Second: {Rate:N1}", finalResult.RecordsPerSecond);
        _logger.LogInformation("   Memory Used: {Memory:N0} MB", finalResult.TotalMemoryUsed);
        _logger.LogInformation("   File System Operations: {Ops:N0}", finalResult.FileSystemOperations);
        
        if (finalResult.TotalDuration.TotalMinutes > 1)
        {
            _logger.LogInformation("   Avg Time per 1000 Records: {Time:ss\\.f}s", 
                TimeSpan.FromSeconds(finalResult.TotalDuration.TotalSeconds / finalResult.TotalCsvRows * 1000));
        }
        
        // Error summary
        if (finalResult.Errors.Any())
        {
            _logger.LogInformation("");
            _logger.LogWarning("⚠️ Error Summary ({Count} errors):", finalResult.Errors.Count);
            
            var errorGroups = finalResult.Errors
                .GroupBy(e => e.Phase)
                .OrderBy(g => g.Key);
                
            foreach (var group in errorGroups)
            {
                _logger.LogWarning("   {Phase}: {Count} errors", group.Key, group.Count());
            }
            
            // Show top error messages
            var topErrors = finalResult.Errors
                .GroupBy(e => e.Message)
                .OrderByDescending(g => g.Count())
                .Take(3);
                
            foreach (var errorGroup in topErrors)
            {
                _logger.LogWarning("   • {Message} ({Count} occurrences)", 
                    errorGroup.Key, errorGroup.Count());
            }
        }
        
        _logger.LogInformation("");
        _logger.LogInformation("═══════════════════════════════════════════════════════════");
    }

    private void LogProgress(ManifestProgress progress)
    {
        var memoryMB = GC.GetTotalMemory(false) / (1024 * 1024);
        
        // Main progress line with all key metrics
        if (progress.TotalCsvRows > 0)
        {
            var etaText = progress.EstimatedTimeRemaining?.ToString(@"mm\:ss") ?? "unknown";
            var completionTime = progress.EstimatedCompletionTime?.ToString("HH:mm:ss") ?? "unknown";
            
            _logger.LogInformation(
                "🔄 Progress: {Percentage:F1}% | {Processed:N0}/{Total:N0} rows | " +
                "{Generated} manifests | {Rate:F1} rec/s | ETA: {ETA} | Memory: {Memory:N0}MB",
                progress.OverallPercentage,
                progress.ProcessedCsvRows,
                progress.TotalCsvRows,
                progress.GeneratedManifestFiles,
                progress.RecordsPerSecond,
                etaText,
                memoryMB);
        }
        else
        {
            _logger.LogInformation(
                "🔄 {Phase}: {Description} | {Generated} manifests | Memory: {Memory:N0}MB",
                GetPhaseIcon(progress.CurrentPhase) + progress.CurrentPhase,
                progress.CurrentPhaseDescription,
                progress.GeneratedManifestFiles,
                memoryMB);
        }
        
        // Additional context for current file/batch if relevant
        if (!string.IsNullOrEmpty(progress.CurrentFile) && progress.TotalBatches > 1)
        {
            _logger.LogDebug("   📁 Current: {File} | Batch {Current}/{Total}", 
                Path.GetFileName(progress.CurrentFile),
                progress.ProcessedBatches + 1,
                progress.TotalBatches);
        }
        
        // Warning if errors are accumulating
        if (progress.ErrorMessages.Count > 0 && progress.ErrorMessages.Count % 10 == 0)
        {
            _logger.LogWarning("   ⚠️ {ErrorCount} errors encountered so far", progress.ErrorMessages.Count);
        }
    }

    private static string GetPhaseIcon(ManifestGenerationPhase phase) => phase switch
    {
        ManifestGenerationPhase.Initializing => "🚀 ",
        ManifestGenerationPhase.LoadingTemplate => "📋 ",
        ManifestGenerationPhase.LoadingCsvData => "📊 ",
        ManifestGenerationPhase.ValidatingData => "🔍 ",
        ManifestGenerationPhase.ProcessingRecords => "⚙️ ",
        ManifestGenerationPhase.GeneratingBatches => "📦 ",
        ManifestGenerationPhase.WritingFiles => "💾 ",
        ManifestGenerationPhase.Finalizing => "🏁 ",
        ManifestGenerationPhase.Completed => "✅ ",
        ManifestGenerationPhase.Failed => "❌ ",
        _ => "📋 "
    };

    public void IncrementFileSystemOperations()
    {
        Interlocked.Increment(ref _totalFileSystemOps);
    }

    public int GetFileSystemOperations() => _totalFileSystemOps;
}
