using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Text.Json;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for loading data from manifest
/// </summary>
public class LoadFromManifestCommandHandler : IRequestHandler<LoadFromManifestCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<LoadFromManifestCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;
    private readonly IOsduClient _osduClient;

    public LoadFromManifestCommandHandler(IMediator mediator, ILogger<LoadFromManifestCommandHandler> logger, IOptions<OsduConfiguration> configuration, IOsduClient osduClient)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
    }

    public async Task<LoadResult> Handle(LoadFromManifestCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            // Handle loading from pre-generated manifest directory (from GenerateManifestsCommand)
            if (request.Manifest == null && !string.IsNullOrEmpty(request.SourcePath))
            {
                _logger.LogInformation("Loading data from pre-generated manifest directory: {SourcePath}", request.SourcePath);
                return await LoadFromManifestDirectory(request.SourcePath, request.DataType, request.FileLocationMapPath, request.IsWorkProduct, cancellationToken);
            }
            
            // Handle loading from explicit manifest object (legacy approach)
            if (request.Manifest != null)
            {
                return await LoadFromManifestObject(request.Manifest, startTime, cancellationToken);
            }

            return new LoadResult
            {
                IsSuccess = false,
                Message = "Either Manifest object or SourcePath must be provided",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = DateTime.UtcNow - startTime
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during manifest load operation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Manifest load operation failed",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.Message
            };
        }
    }

    /// <summary>
    /// Load data from a pre-generated manifest directory (created by GenerateManifestsCommand)
    /// This matches the Python workflow where manifests are generated once, then loaded
    /// </summary>
    private async Task<LoadResult> LoadFromManifestDirectory(string manifestDirectory, TnoDataType dataType, string? fileLocationMapPath, bool isWorkProduct, CancellationToken cancellationToken)
    {
        if (!Directory.Exists(manifestDirectory))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Manifest directory does not exist: {manifestDirectory}",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0
            };
        }

        // Find all JSON manifest files in the directory
        var manifestFiles = Directory.GetFiles(manifestDirectory, "*.json", SearchOption.AllDirectories);
        if (manifestFiles.Length == 0)
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"No manifest files found in directory: {manifestDirectory}",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0
            };
        }

        _logger.LogInformation("Found {ManifestCount} manifest files to process in {Directory}", manifestFiles.Length, manifestDirectory);

        // Pre-scan all manifests to categorize by data type for progress tracking
        var manifestsByType = new Dictionary<TnoDataType, List<string>>();
        var progressTracker = new Dictionary<TnoDataType, ManifestTypeProgress>();

        _logger.LogInformation("Pre-scanning manifests to categorize by data type...");

        foreach (var manifestFile in manifestFiles)
        {
            try
            {
                var manifestJson = await File.ReadAllTextAsync(manifestFile, cancellationToken);
                var manifestObject = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(manifestJson);

                if (manifestObject == null)
                {
                    _logger.LogWarning("Failed to parse manifest file during pre-scan: {ManifestFile}", Path.GetFileName(manifestFile));
                    continue;
                }

                var detectedType = DetectDataTypeFromManifest(manifestObject);
                if (detectedType.HasValue)
                {
                    if (!manifestsByType.ContainsKey(detectedType.Value))
                    {
                        manifestsByType[detectedType.Value] = new List<string>();
                        progressTracker[detectedType.Value] = new ManifestTypeProgress
                        {
                            DataType = detectedType.Value,
                            TotalCount = 0,
                            ProcessedCount = 0,
                            SuccessCount = 0,
                            FailedCount = 0
                        };
                    }
                    manifestsByType[detectedType.Value].Add(manifestFile);
                    progressTracker[detectedType.Value].TotalCount++;
                }
                else
                {
                    _logger.LogWarning("Could not determine data type for manifest: {ManifestFile}", Path.GetFileName(manifestFile));
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during pre-scan of manifest file: {ManifestFile}", manifestFile);
            }
        }

        // Log initial progress summary
        var totalManifests = manifestsByType.Values.Sum(list => list.Count);

        var overallResult = new LoadResult
        {
            IsSuccess = true,
            ProcessedRecords = 0,
            SuccessfulRecords = 0,
            FailedRecords = 0
        };

        // Process manifests by type to maintain consistent logging
        foreach (var typeGroup in manifestsByType.OrderBy(kvp => kvp.Key.ToString()))
        {
            var currentType = typeGroup.Key;
            var filesForType = typeGroup.Value;
            var typeProgress = progressTracker[currentType];

            foreach (var manifestFile in filesForType)
            {
                try
                {
                    typeProgress.ProcessedCount++;

                    // Load and parse the manifest file
                    var manifestJson = await File.ReadAllTextAsync(manifestFile, cancellationToken);
                    var manifestObject = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(manifestJson);

                    if (manifestObject == null)
                    {
                        typeProgress.FailedCount++;
                        overallResult.FailedRecords++;
                        LogProgress(typeProgress, totalManifests, overallResult);
                        continue;
                    }

                    // Load this individual manifest
                    var manifestResult = await LoadFromGeneratedManifest(manifestObject, Path.GetFileName(manifestFile), DateTime.UtcNow, cancellationToken);

                    // Update type-specific progress
                    if (manifestResult.IsSuccess)
                    {
                        typeProgress.SuccessCount++;
                    }
                    else
                    {
                        typeProgress.FailedCount++;
                        _logger.LogWarning("Manifest processing failed for {ManifestFile}: {Message}", 
                            Path.GetFileName(manifestFile), manifestResult.Message);
                    }

                    // Aggregate overall results
                    overallResult.ProcessedRecords += manifestResult.ProcessedRecords;
                    overallResult.SuccessfulRecords += manifestResult.SuccessfulRecords;
                    overallResult.FailedRecords += manifestResult.FailedRecords;
                    overallResult.IsSuccess = overallResult.IsSuccess && manifestResult.IsSuccess;

                    // Log progress after each manifest
                    LogProgress(typeProgress, totalManifests, overallResult);
                }
                catch (Exception ex)
                {
                    typeProgress.FailedCount++;
                    overallResult.FailedRecords++;
                    overallResult.IsSuccess = false;
                    
                    _logger.LogError(ex, "Error processing manifest file: {ManifestFile}", manifestFile);
                    LogProgress(typeProgress, totalManifests, overallResult);
                }
            }

        }

        return overallResult;
    }

    /// <summary>
    /// Load data from an explicit manifest object (original approach)
    /// </summary>
    private async Task<LoadResult> LoadFromManifestObject(LoadingManifest manifest, DateTime startTime, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting data load from manifest with {FileCount} files", 
            manifest.SourceFiles.Length);

        var allRecords = new List<DataRecord>();
        var fileLocationMap = new Dictionary<string, object>();

        // Determine the data type from the manifest kind
        if (!Enum.TryParse<TnoDataType>(manifest.Kind, true, out var dataType))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Invalid data type in manifest: {manifest.Kind}",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = $"Unable to parse data type: {manifest.Kind}"
            };
        }

        // Step 1: Handle file uploads for file-based data types
        if (RequiresFileUpload(dataType))
        {
            _logger.LogInformation("Data type {DataType} requires file upload, uploading files first", dataType);
            
            var fileUploadResult = await _mediator.Send(new UploadFilesCommand(manifest.SourceFiles, ""), cancellationToken);
            
            if (!fileUploadResult.IsSuccess)
            {
                _logger.LogError("File upload failed for {DataType}: {ErrorMessage}", dataType, fileUploadResult.Message);
                return fileUploadResult;
            }
            
            // Extract file location map from upload results if available
            // This would be populated by the UploadFilesCommandHandler
            _logger.LogInformation("File upload completed successfully for {DataType}", dataType);
        }

        // Step 2: Transform all source files to OSDU records
        foreach (var sourceFile in manifest.SourceFiles)
        {
            _logger.LogInformation("Transforming file: {FileName}", sourceFile.FileName);
            
            var records = await _mediator.Send(new TransformDataCommand
            {
                SourceFile = sourceFile,
                DataType = dataType
            }, cancellationToken);

            allRecords.AddRange(records);
        }

        // Step 3: Upload records to OSDU
        return await _mediator.Send(new UploadRecordsCommand
        {
            Records = allRecords
        }, cancellationToken);
    }

    /// <summary>
    /// Determines if a data type requires file upload workflow
    /// </summary>
    private static bool RequiresFileUpload(TnoDataType dataType)
    {
        return dataType switch
        {
            TnoDataType.Documents => true,
            TnoDataType.WellLogs => true,
            TnoDataType.WellMarkers => true,      // May have associated files
            TnoDataType.WellboreTrajectories => true, // May have associated files
            TnoDataType.WorkProducts => true,    // Requires file references
            _ => false
        };
    }

    /// <summary>
    /// Load data from a generated manifest file
    /// Detects data type by checking for ReferenceData, MasterData, or Data properties,
    /// or by examining the 'kind' field for individual records
    /// </summary>
    private async Task<LoadResult> LoadFromGeneratedManifest(Dictionary<string, object> manifestObject, string fileName, DateTime startTime, CancellationToken cancellationToken)
    {
        // Detect data type from manifest structure
        TnoDataType dataType;

        if (manifestObject.ContainsKey("ReferenceData"))
        {
            dataType = TnoDataType.ReferenceData;
        }
        else if (manifestObject.ContainsKey("MasterData"))
        {
            dataType = TnoDataType.MiscMasterData;
        }
        else if (manifestObject.ContainsKey("Data"))
        {
            dataType = TnoDataType.WorkProducts;
        }
        else if (manifestObject.ContainsKey("kind"))
        {
            // This is a single record - detect type from the 'kind' field
            var kindValue = manifestObject["kind"].ToString();
            
            if (kindValue?.Contains("reference-data") == true)
            {
                dataType = TnoDataType.ReferenceData;
            }
            else if (kindValue?.Contains("master-data") == true)
            {
                dataType = TnoDataType.MiscMasterData;
            }
            else if (kindValue?.Contains("work-product") == true || kindValue?.Contains("dataset") == true)
            {
                dataType = TnoDataType.WorkProducts;
            }
            else
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = $"Unknown kind in manifest: {kindValue}",
                    ProcessedRecords = 0,
                    SuccessfulRecords = 0,
                    FailedRecords = 0,
                    Duration = DateTime.UtcNow - startTime,
                    ErrorDetails = $"Unable to determine data type from kind: {kindValue}"
                };
            }
        }
        else
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Invalid data type in manifest: ",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = $"Manifest does not contain ReferenceData, MasterData, Data property, or 'kind' field"
            };
        }

        _logger.LogInformation("Detected data type {DataType} from manifest structure", dataType);
        _logger.LogInformation("Processing manifest {FileName} for {DataType}", fileName, dataType);

        // Upload the manifest to OSDU and let the workflow service handle the actual record processing
        _logger.LogInformation("Uploading {DataType} manifest {FileName} to OSDU workflow service", dataType, fileName);

        // Convert the manifest data to OSDU workflow format and upload
        var uploadResult = await UploadManifestData(manifestObject, dataType, cancellationToken);
        
        return new LoadResult
        {
            IsSuccess = uploadResult.IsSuccess,
            Message = uploadResult.IsSuccess ? 
                $"Successfully submitted {dataType} manifest to OSDU workflow" : 
                $"Failed to submit {dataType} manifest: {uploadResult.Message}",
            ProcessedRecords = 1, // We processed 1 manifest file
            SuccessfulRecords = uploadResult.IsSuccess ? 1 : 0,
            FailedRecords = uploadResult.IsSuccess ? 0 : 1,
            Duration = DateTime.UtcNow - startTime,
            ErrorDetails = uploadResult.ErrorDetails
        };
    }

    /// <summary>
    /// Upload manifest data to OSDU using the workflow API
    /// This matches the Python implementation that calls the workflow endpoint
    /// </summary>
    private async Task<LoadResult> UploadManifestData(Dictionary<string, object> manifestObject, TnoDataType dataType, CancellationToken cancellationToken)
    {
        try
        {
            // Authenticate first
            var authenticated = await _osduClient.AuthenticateAsync(cancellationToken);
            if (!authenticated)
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = "Failed to authenticate with OSDU platform",
                    ErrorDetails = "Authentication failed"
                };
            }

            // Convert manifest to the OSDU workflow format, similar to Python's populate_typed_workflow_request
            var workflowRequest = CreateWorkflowRequest(manifestObject, dataType);
            
            // Upload using the workflow endpoint (matches Python's send_request)
            var result = await _osduClient.SubmitWorkflowAsync(workflowRequest, cancellationToken);
            
            if (!result.IsSuccess || string.IsNullOrEmpty(result.RunId))
            {
                _logger.LogError("Workflow submission failed: Success={IsSuccess}, Message={Message}", 
                    result.IsSuccess, result.Message);
                return result;
            }

            _logger.LogInformation("Workflow submitted successfully with RunId: {RunId}", result.RunId);

            // Poll workflow status until completion (matching Python behavior)
            const int pollingIntervalSeconds = 5;
            const int timeoutSeconds = 60;
            var maxAttempts = timeoutSeconds / pollingIntervalSeconds;
            
            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = "Operation was cancelled during workflow polling",
                        RunId = result.RunId
                    };
                }

                // Wait before checking status (except for first check)
                if (attempt > 0)
                {
                    await Task.Delay(TimeSpan.FromSeconds(pollingIntervalSeconds), cancellationToken);
                }

                try
                {
                    var status = await _osduClient.GetWorkflowStatusAsync(result.RunId, cancellationToken);
                    
                    _logger.LogInformation("Workflow {RunId} status check {Attempt}/{MaxAttempts}: {Status}", 
                        result.RunId, attempt + 1, maxAttempts, status.Status);

                    if (!status.IsRunning)
                    {
                        if (status.IsFinished)
                        {
                            _logger.LogInformation("Workflow {RunId} completed successfully", result.RunId);
                            return new LoadResult
                            {
                                IsSuccess = true,
                                Message = $"Workflow completed successfully. Status: {status.Status}",
                                ProcessedRecords = 1,
                                SuccessfulRecords = 1,
                                FailedRecords = 0,
                                RunId = result.RunId
                            };
                        }
                        else if (status.IsFailed)
                        {
                            var serializeOptions = new JsonSerializerOptions
                            {
                                PropertyNamingPolicy = null, // Preserve original property names
                                WriteIndented = false
                            };
                            var json = JsonSerializer.Serialize(workflowRequest, serializeOptions);
                            _logger.LogError("Workflow {RunId} failed with status: {Status}, Request: {request}", result.RunId, status.Status, json);
                            return new LoadResult
                            {
                                IsSuccess = false,
                                Message = $"Workflow failed with status: {status.Status}",
                                ErrorDetails = status.Status,
                                ProcessedRecords = 1,
                                SuccessfulRecords = 0,
                                FailedRecords = 1,
                                RunId = result.RunId
                            };
                        }
                        else
                        {
                            _logger.LogWarning("Workflow {RunId} in unexpected non-running state: {Status}", result.RunId, status.Status);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error checking workflow status for {RunId}, attempt {Attempt}/{MaxAttempts}", 
                        result.RunId, attempt + 1, maxAttempts);
                }
            }

            // Timeout reached
            _logger.LogWarning("Workflow {RunId} polling timed out after {TimeoutSeconds} seconds", result.RunId, timeoutSeconds);
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Workflow polling timed out after {timeoutSeconds} seconds",
                ErrorDetails = "Timeout waiting for workflow completion",
                ProcessedRecords = 1,
                SuccessfulRecords = 0,
                FailedRecords = 1,
                RunId = result.RunId
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error uploading manifest data to OSDU");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to upload manifest data",
                ErrorDetails = ex.Message
            };
        }
    }

    /// <summary>
    /// Create workflow request matching Python's populate_typed_workflow_request
    /// </summary>
    private object CreateWorkflowRequest(Dictionary<string, object> manifestObject, TnoDataType dataType)
    {
        // Convert TnoDataType to the string format expected by OSDU
        var dataTypeString = dataType switch
        {
            TnoDataType.ReferenceData => "ReferenceData",
            TnoDataType.MiscMasterData => "MasterData", 
            TnoDataType.WorkProducts => "Data",
            _ => dataType.ToString()
        };

        // Create manifest in OSDU format
        var manifest = new Dictionary<string, object>
        {
            ["kind"] = "osdu:wks:Manifest:1.0.0"
        };

        // Add the data based on manifest structure
        if (manifestObject.ContainsKey("ReferenceData"))
        {
            manifest["ReferenceData"] = manifestObject["ReferenceData"];
        }
        else if (manifestObject.ContainsKey("MasterData"))
        {
            manifest["MasterData"] = manifestObject["MasterData"];
        }
        else if (manifestObject.ContainsKey("Data"))
        {
            manifest["Data"] = manifestObject["Data"];
        }
        else
        {
            // Single record case - wrap in appropriate array
            manifest[dataTypeString] = new[] { manifestObject };
        }

        // Create the workflow request matching Python's structure
        return new
        {
            executionContext = new
            {
                Payload = new Dictionary<string, object>
                {
                    ["AppKey"] = "test-app",
                    ["data-partition-id"] = _configuration.DataPartition
                },
                manifest = manifest
            }
        };
    }

    /// <summary>
    /// Helper method to detect data type from manifest structure
    /// </summary>
    private TnoDataType? DetectDataTypeFromManifest(Dictionary<string, object> manifestObject)
    {
        if (manifestObject.ContainsKey("ReferenceData"))
        {
            return TnoDataType.ReferenceData;
        }
        else if (manifestObject.ContainsKey("MasterData"))
        {
            return TnoDataType.MiscMasterData;
        }
        else if (manifestObject.ContainsKey("Data"))
        {
            return TnoDataType.WorkProducts;
        }
        else if (manifestObject.ContainsKey("kind"))
        {
            var kindValue = manifestObject["kind"].ToString();
            
            if (kindValue?.Contains("reference-data") == true)
            {
                return TnoDataType.ReferenceData;
            }
            else if (kindValue?.Contains("master-data") == true)
            {
                return TnoDataType.MiscMasterData;
            }
            else if (kindValue?.Contains("work-product") == true || kindValue?.Contains("dataset") == true)
            {
                return TnoDataType.WorkProducts;
            }
        }
        
        return null;
    }

    /// <summary>
    /// Log progress in the requested format
    /// </summary>
    private void LogProgress(ManifestTypeProgress typeProgress, int totalManifests, LoadResult overallResult)
    {
        _logger.LogInformation("Progress: {ProcessedType}/{TotalType} {DataType}, {FailedType} failed, {SuccessType} success. {ProcessedOverall}/{TotalOverall} overall, {FailedOverall} failed, {SuccessOverall} success",
            typeProgress.ProcessedCount, typeProgress.TotalCount, typeProgress.DataType.ToString().ToLowerInvariant().Replace("miscmasterdata", "master data"),
            typeProgress.FailedCount, typeProgress.SuccessCount,
            overallResult.ProcessedRecords, totalManifests, overallResult.FailedRecords, overallResult.SuccessfulRecords);
    }

    /// <summary>
    /// Helper class to track progress for each manifest type
    /// </summary>
    private class ManifestTypeProgress
    {
        public TnoDataType DataType { get; set; }
        public int TotalCount { get; set; }
        public int ProcessedCount { get; set; }
        public int SuccessCount { get; set; }
        public int FailedCount { get; set; }
    }
}
