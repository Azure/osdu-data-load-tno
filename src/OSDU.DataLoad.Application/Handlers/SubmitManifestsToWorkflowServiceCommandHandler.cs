using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Text.Json;
using System.Threading;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for submitting manifest files to OSDU Workflow Service
/// </summary>
public class SubmitManifestsToWorkflowServiceCommandHandler : IRequestHandler<SubmitManifestsToWorkflowServiceCommand, LoadResult>
{
    private readonly IOsduService _osduService;
    private readonly ILogger<SubmitManifestsToWorkflowServiceCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;

    public SubmitManifestsToWorkflowServiceCommandHandler(
        IOsduService osduService,
        ILogger<SubmitManifestsToWorkflowServiceCommandHandler> logger,
        IOptions<OsduConfiguration> configuration)
    {
        _osduService = osduService ?? throw new ArgumentNullException(nameof(osduService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(SubmitManifestsToWorkflowServiceCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting manifest submission to workflow service");

        try
        {
            if (string.IsNullOrWhiteSpace(request.DataPartition))
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = "Data partition is required",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            // Use the predefined loading sequence and directory mapping from DataLoadingOrder
            _logger.LogInformation("Using predefined DataLoadingOrder for manifest submission");

            // First pass: count total manifests for progress tracking
            var totalManifestCount = 0;
            var dataTypeManifestCounts = new Dictionary<TnoDataType, int>();

            foreach (var dataType in DataLoadingOrder.LoadingSequence)
            {
                if (DataLoadingOrder.ManifestDirectories.TryGetValue(dataType, out var relativePath))
                {
                    var directoryPath = Path.Combine(request.SourceDataPath, relativePath);
                    if (Directory.Exists(directoryPath))
                    {
                        var manifestFiles = Directory.GetFiles(directoryPath, "*.json", SearchOption.AllDirectories);
                        dataTypeManifestCounts[dataType] = manifestFiles.Length;
                        totalManifestCount += manifestFiles.Length;
                    }
                    else
                    {
                        dataTypeManifestCounts[dataType] = 0;
                    }
                }
            }

            _logger.LogInformation("Found {TotalManifestCount} total manifest files to process across {DataTypeCount} data types",
                totalManifestCount, DataLoadingOrder.LoadingSequence.Length);

            if (totalManifestCount == 0)
            {
                return new LoadResult
                {
                    IsSuccess = true,
                    Message = "No manifest files found to process",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            var totalProcessed = 0;
            var totalSuccessful = 0;
            var totalFailed = 0;
            var failedManifests = new List<string>();

            // Process each data type in the predefined LoadingSequence order
            foreach (var dataType in DataLoadingOrder.LoadingSequence)
            {
                var sequenceIndex = Array.IndexOf(DataLoadingOrder.LoadingSequence, dataType);
                var manifestCountForType = dataTypeManifestCounts[dataType];

                _logger.LogInformation("Processing data type: {DataType} (LoadingSequence: {LoadingSequence}) - {ManifestCount} manifests",
                    dataType, sequenceIndex + 1, manifestCountForType);

                // Get the directory path for this data type
                if (!DataLoadingOrder.ManifestDirectories.TryGetValue(dataType, out var relativePath))
                {
                    _logger.LogWarning("No directory mapping found for data type: {DataType}", dataType);
                    continue;
                }

                // Build the full directory path
                var directoryPath = Path.Combine(request.SourceDataPath, relativePath);

                // Check if directory exists
                if (!Directory.Exists(directoryPath))
                {
                    _logger.LogInformation("Directory does not exist for {DataType}: {DirectoryPath}", dataType, directoryPath);
                    continue;
                }

                // Get all manifest files in this directory
                var manifestFiles = Directory.GetFiles(directoryPath, "*.json", SearchOption.AllDirectories);

                if (manifestFiles.Length == 0)
                {
                    _logger.LogInformation("No manifest files found for {DataType} in directory: {DirectoryPath}", dataType, directoryPath);
                    continue;
                }

                var processedInType = 0;
                var successfulInType = 0;
                var failedInType = 0;

                // Check if this data type needs batching
                var needsBatching = dataType == TnoDataType.MiscMasterData ||
                                   dataType == TnoDataType.Wells ||
                                   dataType == TnoDataType.Wellbores;

                if (needsBatching)
                {
                    var result = await ProcessBatchedManifestsAsync(
                        dataType,
                        manifestFiles,
                        sequenceIndex,
                        request.DataPartition,
                        cancellationToken);

                    totalProcessed += result.ProcessedRecords;
                    totalSuccessful += result.SuccessfulRecords;
                    totalFailed += result.FailedRecords;
                    processedInType = result.ProcessedRecords;
                    successfulInType = result.SuccessfulRecords;
                    failedInType = result.FailedRecords;

                    if (result.FailedManifests.Any())
                    {
                        failedManifests.AddRange(result.FailedManifests);
                    }
                }
                else
                {
                    var result = await ProcessIndividualManifestsAsync(
                        dataType,
                        manifestFiles,
                        sequenceIndex,
                        request.DataPartition,
                        totalProcessed,
                        totalManifestCount,
                        cancellationToken);

                    totalProcessed += result.ProcessedRecords;
                    totalSuccessful += result.SuccessfulRecords;
                    totalFailed += result.FailedRecords;
                    processedInType = result.ProcessedRecords;
                    successfulInType = result.SuccessfulRecords;
                    failedInType = result.FailedRecords;

                    if (result.FailedManifests.Any())
                    {
                        failedManifests.AddRange(result.FailedManifests);
                    }
                }

                _logger.LogInformation("Completed data type: {DataType} - Processed: {Processed}, Successful: {Successful}, Failed: {Failed} | Overall Progress: [{TotalProcessed}/{TotalManifests}] ({OverallProgress:F1}%)",
                    dataType, processedInType, successfulInType, failedInType, totalProcessed, totalManifestCount, (double)totalProcessed / totalManifestCount * 100);
            }

            var duration = DateTime.UtcNow - startTime;
            var overallSuccess = totalFailed == 0;

            _logger.LogInformation("Manifest submission completed - Total: {Total}, Successful: {Successful}, Failed: {Failed}, Duration: {Duration:mm\\:ss}",
                totalProcessed, totalSuccessful, totalFailed, duration);

            return new LoadResult
            {
                IsSuccess = overallSuccess,
                ProcessedRecords = totalProcessed,
                SuccessfulRecords = totalSuccessful,
                FailedRecords = totalFailed,
                Duration = duration,
                Message = overallSuccess
                    ? $"Successfully submitted all {totalSuccessful} manifest files to workflow service"
                    : $"Submitted {totalSuccessful} of {totalProcessed} manifest files. Failed: {string.Join(", ", failedManifests)}",
                ErrorDetails = failedManifests.Any()
                    ? $"Failed manifests: {string.Join(", ", failedManifests)}"
                    : string.Empty
            };
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - startTime;
            _logger.LogError(ex, "Error during manifest submission to workflow service");

            return new LoadResult
            {
                IsSuccess = false,
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = duration,
                Message = "Manifest submission failed due to unexpected error",
                ErrorDetails = ex.Message
            };
        }
    }

    private async Task<ProcessingResult> ProcessBatchedManifestsAsync(
        TnoDataType dataType,
        string[] manifestFiles,
        int sequenceIndex,
        string dataPartition,
        CancellationToken cancellationToken)
    {
        var batchSize = _configuration.MasterDataManifestSubmissionBatchSize;

        // Collect all MasterData records from all manifest files
        var allMasterDataRecords = new List<object>();
        var allProcessedFiles = new List<string>();
        _logger.LogInformation("Preparing batch request...this may take a few min");
        foreach (var manifestFile in manifestFiles)
        {
            var fileName = Path.GetFileName(manifestFile);
            allProcessedFiles.Add(fileName);

            try
            {
                // Read and parse the manifest file
                var manifestContent = await File.ReadAllTextAsync(manifestFile, cancellationToken);
                var manifest = JsonSerializer.Deserialize<JsonElement>(manifestContent);

                if (manifest.TryGetProperty("MasterData", out var masterDataProperty) &&
                    masterDataProperty.ValueKind == JsonValueKind.Array)
                {
                    foreach (var record in masterDataProperty.EnumerateArray())
                    {
                        allMasterDataRecords.Add(JsonSerializer.Deserialize<object>(record));
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading manifest file for batching: {FileName}", fileName);
            }
        }

        _logger.LogInformation("Collected {TotalRecords} MasterData records from {FileCount} manifest files for {DataType} (batch size: {BatchSize})",
            allMasterDataRecords.Count, manifestFiles.Length, dataType, batchSize);

        // Process records in configured batches
        var batches = allMasterDataRecords.Chunk(batchSize);
        var batchNumber = 0;
        var processedRecords = 0;
        var successfulRecords = 0;
        var failedRecords = 0;
        var failedManifests = new List<string>();

        foreach (var batch in batches)
        {
            batchNumber++;
            var recordsInBatch = batch.Length;

            try
            {
                _logger.LogInformation("Processing batch {BatchNumber} for {DataType}: {RecordsInBatch} records (LoadingSequence: {LoadingSequence})",
                    batchNumber, dataType, recordsInBatch, sequenceIndex + 1);

                // Create batched manifest
                var batchedManifest = new Dictionary<string, object>
                {
                    ["kind"] = "osdu:wks:Manifest:1.0.0",
                    ["MasterData"] = batch.ToArray()
                };

                // Build the ingest request with the batched manifest content
                var ingestRequest = new
                {
                    executionContext = new
                    {
                        Payload = new Dictionary<string, object>
                        {
                            ["AppKey"] = "test-app",
                            ["data-partition-id"] = dataPartition
                        },
                        manifest = batchedManifest
                    }
                };

                // Submit this batch to the workflow service
                var result = await _osduService.SubmitWorkflowAsync(ingestRequest, cancellationToken);

                if (result.IsSuccess)
                {
                    successfulRecords += recordsInBatch;
                    _logger.LogInformation("✓ Successfully submitted batch {BatchNumber} for {DataType}: {RecordsInBatch} records",
                        batchNumber, dataType, recordsInBatch);
                    await CheckWorkflowStatusAsync(ingestRequest, result.RunId, cancellationToken);
                }
                else
                {
                    failedRecords += recordsInBatch;
                    failedManifests.Add($"{dataType}/batch-{batchNumber} ({recordsInBatch} records) (seq:{sequenceIndex + 1})");
                    _logger.LogError("✗ Failed to submit batch {BatchNumber} for {DataType}: {RecordsInBatch} records - {Error}",
                        batchNumber, dataType, recordsInBatch, result.ErrorDetails);
                }

                processedRecords += recordsInBatch;
            }
            catch (Exception ex)
            {
                failedRecords += recordsInBatch;
                failedManifests.Add($"{dataType}/batch-{batchNumber} ({recordsInBatch} records) (seq:{sequenceIndex + 1})");
                _logger.LogError(ex, "✗ Error processing batch {BatchNumber} for {DataType}: {RecordsInBatch} records",
                    batchNumber, dataType, recordsInBatch);
                processedRecords += recordsInBatch;
            }
        }

        _logger.LogInformation("Completed batching for {DataType}: {FileCount} files containing {TotalRecords} records processed in {BatchCount} batches",
            dataType, allProcessedFiles.Count, allMasterDataRecords.Count, batchNumber);

        return new ProcessingResult
        {
            ProcessedRecords = processedRecords,
            SuccessfulRecords = successfulRecords,
            FailedRecords = failedRecords,
            FailedManifests = failedManifests
        };
    }

    private async Task<ProcessingResult> ProcessIndividualManifestsAsync(
        TnoDataType dataType,
        string[] manifestFiles,
        int sequenceIndex,
        string dataPartition,
        int currentTotalProcessed,
        int totalManifestCount,
        CancellationToken cancellationToken)
    {
        var processedRecords = 0;
        var successfulRecords = 0;
        var failedRecords = 0;
        var failedManifests = new List<string>();

        foreach (var manifestFile in manifestFiles)
        {
            processedRecords++;
            var fileName = Path.GetFileName(manifestFile);

            // Calculate overall progress percentage
            var progressPercentage = (double)(currentTotalProcessed + processedRecords) / totalManifestCount * 100;

            try
            {
                _logger.LogInformation("Processing manifest [{Current}/{Total}] ({Progress:F1}%): {FileName} for {DataType} (LoadingSequence: {LoadingSequence})",
                    currentTotalProcessed + processedRecords, totalManifestCount, progressPercentage, fileName, dataType, sequenceIndex + 1);

                // Read and parse the manifest file
                var manifestContent = await File.ReadAllTextAsync(manifestFile, cancellationToken);
                var manifest = JsonSerializer.Deserialize<Dictionary<string, object>>(manifestContent);
                manifest["kind"] = "osdu:wks:Manifest:1.0.0";

                // Build the ingest request with the manifest content
                var ingestRequest = new
                {
                    executionContext = new
                    {
                        Payload = new Dictionary<string, object>
                        {
                            ["AppKey"] = "test-app",
                            ["data-partition-id"] = dataPartition
                        },
                        manifest
                    }
                };

                // Submit this individual manifest to the workflow service
                var result = await _osduService.SubmitWorkflowAsync(ingestRequest, cancellationToken);

                if (result.IsSuccess)
                {
                    successfulRecords++;
                    _logger.LogInformation("✓ Successfully submitted manifest [{Current}/{Total}] ({Progress:F1}%): {FileName}",
                        currentTotalProcessed + processedRecords, totalManifestCount, progressPercentage, fileName);
                    await CheckWorkflowStatusAsync(ingestRequest, result.RunId, cancellationToken);
                }
                else
                {
                    failedRecords++;
                    failedManifests.Add($"{dataType}/{fileName} (seq:{sequenceIndex + 1})");
                    _logger.LogError("✗ Failed to submit manifest [{Current}/{Total}] ({Progress:F1}%): {FileName} - {Error}",
                        currentTotalProcessed + processedRecords, totalManifestCount, progressPercentage, fileName, result.ErrorDetails);
                }
            }
            catch (Exception ex)
            {
                failedRecords++;
                failedManifests.Add($"{dataType}/{fileName} (seq:{sequenceIndex + 1})");
                _logger.LogError(ex, "✗ Error processing manifest [{Current}/{Total}] ({Progress:F1}%): {FileName}",
                    currentTotalProcessed + processedRecords, totalManifestCount, progressPercentage, fileName);
            }
        }

        return new ProcessingResult
        {
            ProcessedRecords = processedRecords,
            SuccessfulRecords = successfulRecords,
            FailedRecords = failedRecords,
            FailedManifests = failedManifests
        };
    }

    private async Task CheckWorkflowStatusAsync(object request, string runId, CancellationToken cancellationToken)
    {
        // Poll workflow status until completion (matching Python behavior)
        const int pollingIntervalSeconds = 5;
        const int timeoutSeconds = 120;
        var maxAttempts = timeoutSeconds / pollingIntervalSeconds;

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                _logger.LogWarning("Operation was cancelled during workflow polling");
                return;
            }

            // Wait before checking status (except for first check)
            if (attempt > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(pollingIntervalSeconds), cancellationToken);
            }

            try
            {
                var status = await _osduService.GetWorkflowStatusAsync(runId, cancellationToken);

                _logger.LogInformation("Workflow {RunId} status check {Attempt}/{MaxAttempts}: {Status}",
                    runId, attempt + 1, maxAttempts, status.Status);

                if (!status.IsRunning)
                {
                    if (status.IsFinished)
                    {
                        _logger.LogInformation("Workflow {RunId} completed successfully", runId);
                    }
                    else if (status.IsFailed)
                    {
                        var serializeOptions = new JsonSerializerOptions
                        {
                            PropertyNamingPolicy = null, // Preserve original property names
                            WriteIndented = false
                        };
                        var json = JsonSerializer.Serialize(request, serializeOptions);
                        _logger.LogError("Workflow {RunId} failed with status: {Status}, Request: {request}", runId, status.Status, json);
                    }
                    else
                    {
                        _logger.LogWarning("Workflow {RunId} in unexpected non-running state: {Status}", runId, status.Status);
                    }
                    return;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error checking workflow status for {RunId}, attempt {Attempt}/{MaxAttempts}",
                    runId, attempt + 1, maxAttempts);
            }
        }
    }

    private class ProcessingResult
    {
        public int ProcessedRecords { get; init; }
        public int SuccessfulRecords { get; init; }
        public int FailedRecords { get; init; }
        public List<string> FailedManifests { get; init; } = new();
    }
}