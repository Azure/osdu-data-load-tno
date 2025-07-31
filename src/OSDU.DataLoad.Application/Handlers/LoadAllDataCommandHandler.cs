using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for loading all TNO data types in the correct order
/// </summary>
public class LoadAllDataCommandHandler : IRequestHandler<LoadAllDataCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<LoadAllDataCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;

    public LoadAllDataCommandHandler(IMediator mediator, ILogger<LoadAllDataCommandHandler> logger, IOptions<OsduConfiguration> configuration)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(LoadAllDataCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting complete TNO data load operation from {SourcePath}", request.SourcePath);

        if (string.IsNullOrWhiteSpace(request.SourcePath))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Source path is required",
                Duration = DateTime.UtcNow - startTime
            };
        }

        if (!Directory.Exists(request.SourcePath))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Source directory does not exist: {request.SourcePath}",
                Duration = DateTime.UtcNow - startTime
            };
        }

        var overallResult = new LoadResult
        {
            IsSuccess = true,
            ProcessedRecords = 0,
            SuccessfulRecords = 0,
            FailedRecords = 0
        };

        var phaseResults = new List<(TnoDataType DataType, LoadResult Result)>();

        try
        {
            // Prepare stage: Add user to authorization group if specified
            _logger.LogInformation("Starting prepare stage - checking for user authorization setup");
            
            if (!string.IsNullOrWhiteSpace(_configuration.UserEmail))
            {
                var addUserResult = await _mediator.Send(new AddUserToOpsGroupCommand
                {
                    DataPartition = _configuration.DataPartition,
                    UserEmail = _configuration.UserEmail
                }, cancellationToken);

                if (!addUserResult.IsSuccess)
                {
                    _logger.LogWarning("Failed to add user to authorization group, but continuing with data load: {Message}", addUserResult.Message);
                }
            }
            else
            {
                _logger.LogInformation("No user email configured, skipping user authorization setup");
            }

            //// Create legal tag if specified
            //_logger.LogInformation("Creating legal tag");
            //var createLegalTagResult = await _mediator.Send(new CreateLegalTagCommand
            //{
            //    LegalTagName = _configuration.LegalTag
            //}, cancellationToken);

            //if (!createLegalTagResult.IsSuccess)
            //{
            //    _logger.LogWarning("Failed to create legal tag, message: {Message}", createLegalTagResult.Message);
            //    return new LoadResult
            //    {
            //        IsSuccess = false,
            //        Message = $"Failed to create legal tag {createLegalTagResult.Message}",
            //        Duration = DateTime.UtcNow - startTime
            //    };
            //}

            //Step 1: Upload Dataset Files(corresponds to LoadFiles in Python)
            //Only upload actual files -not reference / master data which are pure records
            //_logger.LogInformation("Step 1: Uploading dataset files (documents, well-logs, markers, trajectories)");
            //var uploadStartTime = DateTime.UtcNow;
            //var uploadResult = await _mediator.Send(new UploadDatasetsCommand
            //{
            //    SourceDataPath = request.SourcePath,
            //    OutputPath = Path.Combine(request.SourcePath, "output")
            //}, cancellationToken);

            //if (!uploadResult.IsSuccess)
            //{
            //    _logger.LogError("Dataset file upload failed: {Message}", uploadResult.Message);
            //    return new LoadResult
            //    {
            //        IsSuccess = false,
            //        Message = $"Dataset file upload failed: {uploadResult.Message}",
            //        Duration = DateTime.UtcNow - startTime,
            //        ErrorDetails = uploadResult.ErrorDetails
            //    };
            //}

            //_logger.LogInformation("Step 1 completed in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} files uploaded",
            //    DateTime.UtcNow - uploadStartTime, uploadResult.SuccessfulRecords, uploadResult.ProcessedRecords);

            // Step 2: Generate Manifests(corresponds to GenerateManifests in Python)
            //_logger.LogInformation("Step 2: Generating manifests from CSV data");
            //var manifestStartTime = DateTime.UtcNow;
            //var manifestResult = await _mediator.Send(new GenerateManifestsCommand
            //{
            //    SourceDataPath = request.SourcePath,
            //    OutputPath = request.SourcePath,
            //    DataPartition = _configuration.DataPartition,
            //    AclOwner = _configuration.AclOwner,
            //    AclViewer = _configuration.AclViewer,
            //    LegalTag = _configuration.LegalTag
            //}, cancellationToken);

            //if (!manifestResult.IsSuccess)
            //{
            //    _logger.LogError("Manifest generation failed: {Message}", manifestResult.Message);
            //    return new LoadResult
            //    {
            //        IsSuccess = false,
            //        Message = $"Manifest generation failed: {manifestResult.Message}",
            //        Duration = DateTime.UtcNow - startTime,
            //        ErrorDetails = manifestResult.ErrorDetails
            //    };
            //}

            //_logger.LogInformation("Step 2 completed in {Duration:mm\\:ss} - {SuccessfulRecords} manifest groups generated",
            //    DateTime.UtcNow - manifestStartTime, manifestResult.SuccessfulRecords);

            //// Step 3: Load Master Data (Reference and Master Data manifests)
            //_logger.LogInformation("Step 3: Loading reference and master data manifests");
            //var masterDataStartTime = DateTime.UtcNow;

            //var masterDataTypes = new[]
            //{
            //    TnoDataType.ReferenceData,
            //    TnoDataType.MiscMasterData,
            //    TnoDataType.Wells,
            //    TnoDataType.Wellbores
            //};

            //// Pre-scan to get total manifest counts for overall progress tracking
            //var totalManifestCounts = await GetTotalManifestCounts(request.SourcePath, masterDataTypes);
            //var overallProgress = new OverallProgress
            //{
            //    TotalManifests = totalManifestCounts.Values.Sum(),
            //    ProcessedManifests = 0,
            //    SuccessfulManifests = 0,
            //    FailedManifests = 0,
            //    TypeCounts = totalManifestCounts,
            //    StartTime = startTime // Use the operation start time
            //};

            //_logger.LogInformation("🔍 Overall progress initialization:");
            //foreach (var kvp in totalManifestCounts.Where(x => x.Value > 0))
            //{
            //    _logger.LogInformation("  📋 {DataType}: {Count} manifests", kvp.Key, kvp.Value);
            //}
            //_logger.LogInformation("🎯 Total: {TotalCount} manifests to process across all types", overallProgress.TotalManifests);

            //foreach (var dataType in masterDataTypes)
            //{
            //    var typeManifestCount = overallProgress.TypeCounts.GetValueOrDefault(dataType, 0);
            //    _logger.LogInformation("🚀 Loading {DataType} master data ({Count} manifests)", dataType, typeManifestCount);

            //    // Check if the subdirectory exists for this data type
            //    var subdirectory = DataLoadingOrder.DirectoryMapping[dataType];
            //    var dataTypePath = Path.Combine(request.SourcePath, subdirectory);

            //    if (!Directory.Exists(dataTypePath))
            //    {
            //        _logger.LogWarning("⚠️ Skipping {DataType} - directory not found: {Path}", dataType, dataTypePath);
            //        continue;
            //    }

            //    // Load data for this type
            //    var phaseStartTime = DateTime.UtcNow;
            //    overallProgress.TypeStartTimes[dataType] = phaseStartTime;

            //    try
            //    {
            //        var result = await _mediator.Send(new LoadDataCommand
            //        {
            //            SourcePath = dataTypePath,
            //            DataType = dataType
            //        }, cancellationToken);

            //        phaseResults.Add((dataType, result));

            //        // Update overall progress tracking
            //        overallProgress.ProcessedManifests += result.ProcessedRecords;
            //        overallProgress.SuccessfulManifests += result.SuccessfulRecords;
            //        overallProgress.FailedManifests += result.FailedRecords;

            //        // Aggregate results
            //        overallResult = new LoadResult
            //        {
            //            IsSuccess = overallResult.IsSuccess && result.IsSuccess,
            //            ProcessedRecords = overallResult.ProcessedRecords + result.ProcessedRecords,
            //            SuccessfulRecords = overallResult.SuccessfulRecords + result.SuccessfulRecords,
            //            FailedRecords = overallResult.FailedRecords + result.FailedRecords,
            //            Duration = DateTime.UtcNow - startTime
            //        };

            //        var phaseTime = DateTime.UtcNow - phaseStartTime;
            //        overallProgress.TypeDurations[dataType] = phaseTime;

            //        // Enhanced progress reporting with visual indicators and ETA
            //        _logger.LogInformation("✅ Completed {DataType} in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} records successful",
            //            dataType, phaseTime, result.SuccessfulRecords, result.ProcessedRecords);

            //        var progressPercentage = overallProgress.TotalManifests > 0
            //            ? (double)overallProgress.ProcessedManifests / overallProgress.TotalManifests * 100
            //            : 0;

            //        var etaText = overallProgress.EstimatedTimeRemaining?.ToString(@"mm\:ss") ?? "unknown";

            //        _logger.LogInformation("📊 Overall Progress: {Percentage:F1}% | {Processed}/{Total} manifests | {Failed} failed | {Success} successful | ETA: {ETA}",
            //            progressPercentage, overallProgress.ProcessedManifests, overallProgress.TotalManifests,
            //            overallProgress.FailedManifests, overallProgress.SuccessfulManifests, etaText);

            //        if (!result.IsSuccess)
            //        {
            //            _logger.LogError("❌ Master data loading failed for {DataType}: {Message}", dataType, result.Message);
            //            return new LoadResult
            //            {
            //                IsSuccess = false,
            //                Message = $"Master data loading failed for {dataType}: {result.Message}",
            //                Duration = DateTime.UtcNow - startTime,
            //                ErrorDetails = result.ErrorDetails,
            //                ProcessedRecords = overallResult.ProcessedRecords + result.ProcessedRecords,
            //                SuccessfulRecords = overallResult.SuccessfulRecords + result.SuccessfulRecords,
            //                FailedRecords = overallResult.FailedRecords + result.FailedRecords
            //            };
            //        }
            //    }
            //    catch (Exception ex)
            //    {
            //        _logger.LogError(ex, "💥 Failed to load {DataType} from {Path}", dataType, dataTypePath);

            //        return new LoadResult
            //        {
            //            IsSuccess = false,
            //            Message = $"Critical error loading {dataType}: {ex.Message}",
            //            Duration = DateTime.UtcNow - startTime,
            //            ErrorDetails = ex.Message,
            //            ProcessedRecords = overallResult.ProcessedRecords,
            //            SuccessfulRecords = overallResult.SuccessfulRecords,
            //            FailedRecords = overallResult.FailedRecords + 1
            //        };
            //    }
            //}

            //_logger.LogInformation("Step 3 completed in {Duration:mm\\:ss}", DateTime.UtcNow - masterDataStartTime);

            // Step 4: Load Work Products (using file location maps from step 1)
            _logger.LogInformation("Step 4: Loading work product manifests");
            var workProductStartTime = DateTime.UtcNow;

            var workProductTypes = new[]
            {
                TnoDataType.Documents,
                TnoDataType.WellLogs,
                TnoDataType.WellMarkers,
                TnoDataType.WellboreTrajectories
            };

            var outputPath = Path.Combine(request.SourcePath, "output");
            var fileLocationMappings = new Dictionary<TnoDataType, string>
            {
                { TnoDataType.Documents, Path.Combine(outputPath, "loaded-documents-datasets.json") },
                { TnoDataType.WellLogs, Path.Combine(outputPath, "loaded-welllogs-datasets.json") },
                { TnoDataType.WellMarkers, Path.Combine(outputPath, "loaded-marker-datasets.json") },
                { TnoDataType.WellboreTrajectories, Path.Combine(outputPath, "loaded-trajectories-datasets.json") }
            };



            foreach (var dataType in workProductTypes)
            {
                _logger.LogInformation("Loading {DataType} work products", dataType);

                // Check if the subdirectory exists for this data type
                var subdirectory = DataLoadingOrder.DirectoryMapping[dataType];
                var dataTypePath = Path.Combine(request.SourcePath, subdirectory);

                if (!Directory.Exists(dataTypePath))
                {
                    _logger.LogWarning("Skipping {DataType} - directory not found: {Path}", dataType, dataTypePath);
                    continue;
                }

                // Check if file location mapping exists
                var fileLocationMap = fileLocationMappings.GetValueOrDefault(dataType);
                if (string.IsNullOrEmpty(fileLocationMap) || !File.Exists(fileLocationMap))
                {
                    _logger.LogWarning("Skipping {DataType} - file location mapping not found: {Map}", dataType, fileLocationMap);
                    continue;
                }

                // loop json dir✅
                // loop through files
                var manifestFiles = Directory.GetFiles(dataTypePath, "*.json", SearchOption.AllDirectories);
                foreach (var manifestFile in manifestFiles) {
                    // get data property
                    var manifestJson = await File.ReadAllTextAsync(manifestFile, cancellationToken);
                    var manifestObject = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(manifestJson);

                    if (manifestObject == null)
                    {
                        _logger.LogWarning("Failed to parse manifest file during pre-scan: {ManifestFile}", Path.GetFileName(manifestFile));
                        continue;
                    }




                    var data = manifestObject["Data"];

                    


                    var manifest = new Dictionary<string, object>
                    {
                        ["Data"] = data
                    };
                    Console.WriteLine(manifest);
                    // update data property with values from map file (update_work_products_metadata)
                    // wrap data property in { "kind": "osdu:wks:Manifest:1.0.0",  "data": <object>}
                    // wrap in execution context


                    var ingestRequest = new
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

                    Console.WriteLine(ingestRequest);
                }
            }




            //foreach (var dataType in workProductTypes)
            //{
            //    _logger.LogInformation("Loading {DataType} work products", dataType);

            //    // Check if the subdirectory exists for this data type
            //    var subdirectory = DataLoadingOrder.DirectoryMapping[dataType];
            //    var dataTypePath = Path.Combine(request.SourcePath, subdirectory);

            //    if (!Directory.Exists(dataTypePath))
            //    {
            //        _logger.LogWarning("Skipping {DataType} - directory not found: {Path}", dataType, dataTypePath);
            //        continue;
            //    }

            //    // Check if file location mapping exists
            //    var fileLocationMap = fileLocationMappings.GetValueOrDefault(dataType);
            //    if (string.IsNullOrEmpty(fileLocationMap) || !File.Exists(fileLocationMap))
            //    {
            //        _logger.LogWarning("Skipping {DataType} - file location mapping not found: {Map}", dataType, fileLocationMap);
            //        continue;
            //    }

            //    // Load data for this type
            //    var phaseStartTime = DateTime.UtcNow;

            //    try
            //    {
            //        // For work products, pass file location mappings for manifest processing
            //        var result = await _mediator.Send(new LoadDataCommand
            //        {
            //            SourcePath = dataTypePath,
            //            DataType = dataType,
            //            FileLocationMappings = fileLocationMappings
            //        }, cancellationToken);

            //        phaseResults.Add((dataType, result));

            //        // Aggregate results
            //        overallResult = new LoadResult
            //        {
            //            IsSuccess = overallResult.IsSuccess && result.IsSuccess,
            //            ProcessedRecords = overallResult.ProcessedRecords + result.ProcessedRecords,
            //            SuccessfulRecords = overallResult.SuccessfulRecords + result.SuccessfulRecords,
            //            FailedRecords = overallResult.FailedRecords + result.FailedRecords,
            //            Duration = DateTime.UtcNow - startTime
            //        };

            //        var phaseTime = DateTime.UtcNow - phaseStartTime;
            //        _logger.LogInformation("Completed {DataType} work products in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} records successful",
            //            dataType, phaseTime, result.SuccessfulRecords, result.ProcessedRecords);

            //        if (!result.IsSuccess)
            //        {
            //            _logger.LogError("Work product loading failed for {DataType}: {Message}", dataType, result.Message);
            //            return new LoadResult
            //            {
            //                IsSuccess = false,
            //                Message = $"Work product loading failed for {dataType}: {result.Message}",
            //                Duration = DateTime.UtcNow - startTime,
            //                ErrorDetails = result.ErrorDetails,
            //                ProcessedRecords = overallResult.ProcessedRecords + result.ProcessedRecords,
            //                SuccessfulRecords = overallResult.SuccessfulRecords + result.SuccessfulRecords,
            //                FailedRecords = overallResult.FailedRecords + result.FailedRecords
            //            };
            //        }
            //    }
            //    catch (Exception ex)
            //    {
            //        _logger.LogError(ex, "Failed to load {DataType} work products from {Path}", dataType, dataTypePath);

            //        return new LoadResult
            //        {
            //            IsSuccess = false,
            //            Message = $"Critical error loading {dataType} work products: {ex.Message}",
            //            Duration = DateTime.UtcNow - startTime,
            //            ErrorDetails = ex.Message,
            //            ProcessedRecords = overallResult.ProcessedRecords,
            //            SuccessfulRecords = overallResult.SuccessfulRecords,
            //            FailedRecords = overallResult.FailedRecords + 1
            //        };
            //    }
            //}

            _logger.LogInformation("Step 4 completed in {Duration:mm\\:ss}", DateTime.UtcNow - workProductStartTime);

            // Generate summary message and enhanced completion report
            var summary = GenerateSummaryMessage(phaseResults, overallResult);
            
            // Log comprehensive completion summary
            //LogCompletionSummary(overallResult, overallProgress, phaseResults, DateTime.UtcNow - startTime);
            
            overallResult = new LoadResult
            {
                IsSuccess = overallResult.IsSuccess,
                ProcessedRecords = overallResult.ProcessedRecords,
                SuccessfulRecords = overallResult.SuccessfulRecords,
                FailedRecords = overallResult.FailedRecords,
                Message = summary,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = overallResult.ErrorDetails
            };

            _logger.LogInformation("🎉 Completed TNO data load operation in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} total records successful",
                overallResult.Duration, overallResult.SuccessfulRecords, overallResult.ProcessedRecords);

            return overallResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Critical error during TNO data load operation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Critical error during data load operation",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }

    private string GenerateSummaryMessage(List<(TnoDataType DataType, LoadResult Result)> phaseResults, LoadResult overallResult)
    {
        var summary = new List<string>
        {
            $"TNO Data Load Complete - {(overallResult.IsSuccess ? "SUCCESS" : "COMPLETED WITH ERRORS")}",
            "",
            "Phase Results:"
        };

        foreach (var (dataType, result) in phaseResults)
        {
            var status = result.IsSuccess ? "✅" : "❌";
            var rate = result.ProcessedRecords > 0 ? (double)result.SuccessfulRecords / result.ProcessedRecords * 100 : 0;
            summary.Add($"  {status} {dataType}: {result.SuccessfulRecords}/{result.ProcessedRecords} records ({rate:F1}%) in {result.Duration:mm\\:ss}");
        }

        summary.Add("");
        summary.Add($"Total: {overallResult.SuccessfulRecords}/{overallResult.ProcessedRecords} records in {overallResult.Duration:mm\\:ss}");

        if (overallResult.ProcessedRecords > 0)
        {
            var overallRate = (double)overallResult.SuccessfulRecords / overallResult.ProcessedRecords * 100;
            summary.Add($"Overall Success Rate: {overallRate:F1}%");
        }

        return string.Join(Environment.NewLine, summary);
    }

    /// <summary>
    /// Log comprehensive completion summary with enhanced visual formatting
    /// </summary>
    private void LogCompletionSummary(LoadResult overallResult, OverallProgress overallProgress, 
        List<(TnoDataType dataType, LoadResult result)> phaseResults, TimeSpan totalDuration)
    {
        var icon = overallResult.IsSuccess ? "✅" : "❌";
        var status = overallResult.IsSuccess ? "SUCCESS" : "FAILED";
        
        _logger.LogInformation("");
        _logger.LogInformation("═══════════════════════════════════════════════════════════");
        _logger.LogInformation("{Icon} TNO DATA LOAD COMPLETE", icon);
        _logger.LogInformation("═══════════════════════════════════════════════════════════");
        _logger.LogInformation("");
        
        // Overall results
        _logger.LogInformation("📊 Overall Results:");
        _logger.LogInformation("   Status: {Status}", status);
        _logger.LogInformation("   Total Duration: {Duration:mm\\:ss\\.fff}", totalDuration);
        _logger.LogInformation("   Data Types Processed: {Count}", phaseResults.Count);
        _logger.LogInformation("");
        
        // Record statistics
        _logger.LogInformation("📈 Record Processing:");
        _logger.LogInformation("   Total Records: {Total:N0}", overallResult.ProcessedRecords);
        _logger.LogInformation("   Successful Records: {Successful:N0}", overallResult.SuccessfulRecords);
        _logger.LogInformation("   Failed Records: {Failed:N0}", overallResult.FailedRecords);
        if (overallResult.ProcessedRecords > 0)
        {
            var successRate = (double)overallResult.SuccessfulRecords / overallResult.ProcessedRecords;
            _logger.LogInformation("   Success Rate: {Rate:P1}", successRate);
        }
        _logger.LogInformation("");
        
        // Manifest statistics
        _logger.LogInformation("📋 Manifest Processing:");
        _logger.LogInformation("   Total Manifests: {Total:N0}", overallProgress.TotalManifests);
        _logger.LogInformation("   Processed Manifests: {Processed:N0}", overallProgress.ProcessedManifests);
        _logger.LogInformation("   Successful Manifests: {Successful:N0}", overallProgress.SuccessfulManifests);
        _logger.LogInformation("   Failed Manifests: {Failed:N0}", overallProgress.FailedManifests);
        _logger.LogInformation("");
        
        // Performance metrics
        if (totalDuration.TotalSeconds > 0)
        {
            var recordsPerSecond = overallResult.ProcessedRecords / totalDuration.TotalSeconds;
            var manifestsPerSecond = overallProgress.ProcessedManifests / totalDuration.TotalSeconds;
            
            _logger.LogInformation("⚡ Performance Metrics:");
            _logger.LogInformation("   Records/Second: {Rate:N1}", recordsPerSecond);
            _logger.LogInformation("   Manifests/Second: {Rate:N1}", manifestsPerSecond);
            if (totalDuration.TotalMinutes > 1)
            {
                _logger.LogInformation("   Avg Time per 1000 Records: {Time:ss\\.f}s", 
                    TimeSpan.FromSeconds(totalDuration.TotalSeconds / overallResult.ProcessedRecords * 1000));
            }
            _logger.LogInformation("");
        }
        
        // Data type breakdown
        _logger.LogInformation("📂 Data Type Breakdown:");
        foreach (var (dataType, result) in phaseResults.OrderBy(x => x.dataType.ToString()))
        {
            var typeIcon = result.IsSuccess ? "✅" : "❌";
            var typeSuccessRate = result.ProcessedRecords > 0 
                ? (double)result.SuccessfulRecords / result.ProcessedRecords * 100 
                : 0;
            
            _logger.LogInformation("   {Icon} {DataType}: {Success}/{Total} records ({Rate:F1}%) in {Duration:mm\\:ss}",
                typeIcon, dataType, result.SuccessfulRecords, result.ProcessedRecords, 
                typeSuccessRate, result.Duration);
        }
        
        // Error summary if any failures
        if (overallResult.FailedRecords > 0 || !overallResult.IsSuccess)
        {
            _logger.LogInformation("");
            _logger.LogWarning("⚠️ Issues Summary:");
            
            if (overallResult.FailedRecords > 0)
            {
                _logger.LogWarning("   Total Failed Records: {Failed:N0}", overallResult.FailedRecords);
            }
            
            var failedTypes = phaseResults.Where(x => !x.result.IsSuccess || x.result.FailedRecords > 0);
            foreach (var (dataType, result) in failedTypes)
            {
                if (!result.IsSuccess)
                {
                    _logger.LogWarning("   ❌ {DataType}: Operation failed - {Message}", dataType, result.Message);
                }
                else if (result.FailedRecords > 0)
                {
                    _logger.LogWarning("   ⚠️ {DataType}: {Failed} failed records out of {Total}", 
                        dataType, result.FailedRecords, result.ProcessedRecords);
                }
            }
        }
        
        _logger.LogInformation("");
        _logger.LogInformation("═══════════════════════════════════════════════════════════");
        _logger.LogInformation("");
    }

    /// <summary>
    /// Pre-scan all directories to count total manifests for progress tracking
    /// </summary>
    private async Task<Dictionary<TnoDataType, int>> GetTotalManifestCounts(string sourcePath, TnoDataType[] dataTypes)
    {
        var counts = new Dictionary<TnoDataType, int>();

        foreach (var dataType in dataTypes)
        {
            try
            {
                var subdirectory = DataLoadingOrder.DirectoryMapping[dataType];
                var dataTypePath = Path.Combine(sourcePath, subdirectory);

                if (Directory.Exists(dataTypePath))
                {
                    var manifestFiles = Directory.GetFiles(dataTypePath, "*.json", SearchOption.AllDirectories);
                    counts[dataType] = manifestFiles.Length;
                }
                else
                {
                    counts[dataType] = 0;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error counting manifests for {DataType}", dataType);
                counts[dataType] = 0;
            }
        }

        return counts;
    }

    /// <summary>
    /// Enhanced helper class to track overall progress across all data types
    /// </summary>
    private class OverallProgress
    {
        public int TotalManifests { get; set; }
        public int ProcessedManifests { get; set; }
        public int SuccessfulManifests { get; set; }
        public int FailedManifests { get; set; }
        public Dictionary<TnoDataType, int> TypeCounts { get; set; } = new();
        
        // Additional metrics for enhanced tracking
        public DateTime StartTime { get; set; } = DateTime.UtcNow;
        public Dictionary<TnoDataType, DateTime> TypeStartTimes { get; set; } = new();
        public Dictionary<TnoDataType, TimeSpan> TypeDurations { get; set; } = new();
        
        /// <summary>
        /// Calculate overall completion percentage
        /// </summary>
        public double CompletionPercentage => TotalManifests > 0 
            ? (double)ProcessedManifests / TotalManifests * 100 
            : 0;
        
        /// <summary>
        /// Calculate estimated time remaining based on current processing rate
        /// </summary>
        public TimeSpan? EstimatedTimeRemaining
        {
            get
            {
                if (ProcessedManifests == 0 || TotalManifests == 0 || ProcessedManifests >= TotalManifests)
                    return null;
                
                var elapsed = DateTime.UtcNow - StartTime;
                var avgTimePerManifest = elapsed.TotalSeconds / ProcessedManifests;
                var remainingManifests = TotalManifests - ProcessedManifests;
                
                return TimeSpan.FromSeconds(remainingManifests * avgTimePerManifest);
            }
        }
    }
}
