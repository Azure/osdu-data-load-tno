using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using System.Text.Json;

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
                    var jsonBefore = JsonSerializer.Serialize(data);
                    Console.WriteLine(jsonBefore);
                    // Update work product data (equivalent to Python's update_work_products_metadata)
                    var updatedData = await UpdateWorkProductsMetadata(data, fileLocationMap, request.SourcePath);

                    var jsonAfter = JsonSerializer.Serialize(updatedData);
                    Console.WriteLine(jsonAfter);

                    var manifest = new Dictionary<string, object>
                    {
                        ["Data"] = updatedData
                    };
                    Console.WriteLine(manifest);


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

                    var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
                    var ingestJson = JsonSerializer.Serialize(ingestRequest, jsonOptions);
                    Console.WriteLine(ingestJson);
                    
                    // Write ingestRequest to manifests/work-products directory
                    var workProductsDir = Path.Combine(request.SourcePath, "manifests", "work-products");
                    Directory.CreateDirectory(workProductsDir);
                    
                    var manifestFileName = Path.GetFileNameWithoutExtension(manifestFile);
                    var outputFileName = $"{manifestFileName}_ingest.json";
                    var outputFilePath = Path.Combine(workProductsDir, outputFileName);
                    
                    await File.WriteAllTextAsync(outputFilePath, ingestJson, cancellationToken);
                    _logger.LogInformation("Wrote work product manifest to: {OutputPath}", outputFilePath);
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

        /// <summary>
        /// C# implementation of Python's update_work_products_metadata function
        /// </summary>
        private async Task<object> UpdateWorkProductsMetadata(object data, string fileLocationMapPath, string baseDir)
        {
            try
            {
                // Create namespace patterns (equivalent to Python's reference_pattern and master_pattern)
                var referencePattern = $"{_configuration.DataPartition}:reference-data";
                var masterPattern = $"{_configuration.DataPartition}:master-data";

                // Convert data to JSON string for pattern replacements (equivalent to Python's json.dumps + replace)
                var dataJson = System.Text.Json.JsonSerializer.Serialize(data);
                
                var updatedManifest = dataJson
                    .Replace("osdu:reference-data", referencePattern)
                    .Replace("osdu:master-data", masterPattern)
                    .Replace("surrogate-key:file-1", "surrogate-key:dataset--1:0:0")
                    .Replace("surrogate-key:wpc-1", "surrogate-key:wpc--1:0:0");

                // Parse back to object (equivalent to Python's json.loads)
                var updatedData = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(updatedManifest);
                
                if (updatedData == null)
                {
                    _logger.LogWarning("Failed to deserialize updated manifest data");
                    return data;
                }

                _logger.LogDebug("Base directory is {BaseDir}", baseDir);

                // Update legal and ACL tags (equivalent to Python's update_legal_and_acl_tags and add_metadata calls)
                UpdateLegalAndAclTags(updatedData, "WorkProduct");
                AddMetadata(updatedData, "WorkProductComponents");
                AddMetadata(updatedData, "Datasets");

                // Load file location map (equivalent to Python's "with open(file_location_map) as file")
                if (!File.Exists(fileLocationMapPath))
                {
                    _logger.LogWarning("File location map not found: {Path}", fileLocationMapPath);
                    return updatedData;
                }

                var locationMapJson = await File.ReadAllTextAsync(fileLocationMapPath);
                var locationMap = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, object>>>(locationMapJson);

                if (locationMap == null)
                {
                    _logger.LogWarning("Failed to parse file location map");
                    return updatedData;
                }

                // Get file name from WorkProduct data (equivalent to Python's file_name = data["WorkProduct"]["data"]["Name"])
                if (updatedData.TryGetValue("WorkProduct", out var workProductObj))
                {
                    string? fileName = null;
                    
                    // Handle both JsonElement and already deserialized object cases
                    if (workProductObj is JsonElement workProductElement)
                    {
                        if (workProductElement.TryGetProperty("data", out var workProductData) &&
                            workProductData.TryGetProperty("Name", out var nameElement))
                        {
                            fileName = nameElement.GetString();
                        }
                    }
                    else if (workProductObj is Dictionary<string, object> workProductDict)
                    {
                        if (workProductDict.TryGetValue("data", out var dataObj))
                        {
                            if (dataObj is JsonElement dataElement && dataElement.TryGetProperty("Name", out var nameEl))
                            {
                                fileName = nameEl.GetString();
                            }
                            else if (dataObj is Dictionary<string, object> dataDict && dataDict.TryGetValue("Name", out var nameObj))
                            {
                                fileName = nameObj?.ToString();
                            }
                            else if (dataObj is string dataString)
                            {
                                // Handle case where data is a JSON string
                                try
                                {
                                    var parsedData = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(dataString);
                                    if (parsedData?.TryGetValue("Name", out var nameValue) == true)
                                    {
                                        fileName = nameValue?.ToString();
                                    }
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogWarning(ex, "Failed to parse WorkProduct data JSON string");
                                }
                            }
                        }
                    }
                    
                    if (!string.IsNullOrEmpty(fileName) && locationMap.ContainsKey(fileName))
                    {
                        var fileInfo = locationMap[fileName];
                        
                        // Extract file information
                        var fileSource = fileInfo.TryGetValue("file_source", out var fs) ? fs.ToString() : "";
                        var fileId = fileInfo.TryGetValue("file_id", out var fi) ? fi.ToString() : "";
                        var fileVersion = fileInfo.TryGetValue("file_record_version", out var fv) ? fv.ToString() : "";

                        // Update Dataset with Generated File Id and File Source
                        if (updatedData.TryGetValue("Datasets", out var datasetsObj))
                        {
                            List<Dictionary<string, object>>? datasetsDict = null;
                            
                            // Handle both JsonElement and already deserialized List cases
                            if (datasetsObj is JsonElement datasets && 
                                datasets.ValueKind == JsonValueKind.Array && 
                                datasets.GetArrayLength() > 0)
                            {
                                datasetsDict = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(datasets.GetRawText());
                            }
                            else if (datasetsObj is List<Dictionary<string, object>> existingList && existingList.Count > 0)
                            {
                                datasetsDict = existingList;
                            }
                            
                            if (datasetsDict != null && datasetsDict.Count > 0)
                            {
                                datasetsDict[0]["id"] = fileId;
                                
                                // Update FileSource and remove PreloadFilePath
                                if (datasetsDict[0].TryGetValue("data", out var dataObj))
                                {
                                    Dictionary<string, object>? dataDict = null;
                                    
                                    if (dataObj is JsonElement dataElement)
                                    {
                                        dataDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(dataElement.GetRawText());
                                    }
                                    else if (dataObj is Dictionary<string, object> existingDataDict)
                                    {
                                        dataDict = existingDataDict;
                                    }
                                    
                                    if (dataDict?.TryGetValue("DatasetProperties", out var datasetPropsObj) == true)
                                    {
                                        Dictionary<string, object>? propsDict = null;
                                        
                                        if (datasetPropsObj is JsonElement datasetProps)
                                        {
                                            propsDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(datasetProps.GetRawText());
                                        }
                                        else if (datasetPropsObj is Dictionary<string, object> existingPropsDict)
                                        {
                                            propsDict = existingPropsDict;
                                        }
                                        
                                        if (propsDict?.TryGetValue("FileSourceInfo", out var fileSourceInfoObj) == true)
                                        {
                                            Dictionary<string, object>? fileSourceDict = null;
                                            
                                            if (fileSourceInfoObj is JsonElement fileSourceInfo)
                                            {
                                                fileSourceDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(fileSourceInfo.GetRawText());
                                            }
                                            else if (fileSourceInfoObj is Dictionary<string, object> existingFileSourceDict)
                                            {
                                                fileSourceDict = existingFileSourceDict;
                                            }
                                            
                                            if (fileSourceDict != null)
                                            {
                                                fileSourceDict["FileSource"] = fileSource;
                                                fileSourceDict.Remove("PreloadFilePath");
                                                propsDict["FileSourceInfo"] = fileSourceDict;
                                                dataDict["DatasetProperties"] = propsDict;
                                                datasetsDict[0]["data"] = dataDict;
                                            }
                                        }
                                    }
                                }
                                
                                updatedData["Datasets"] = datasetsDict;
                            }
                        }

                        // Update FileId in WorkProductComponent
                        if (updatedData.TryGetValue("WorkProductComponents", out var wpcObj))
                        {
                            List<Dictionary<string, object>>? wpcList = null;
                            
                            // Handle both JsonElement and already deserialized List cases
                            if (wpcObj is JsonElement wpc && 
                                wpc.ValueKind == JsonValueKind.Array && 
                                wpc.GetArrayLength() > 0)
                            {
                                wpcList = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(wpc.GetRawText());
                            }
                            else if (wpcObj is List<Dictionary<string, object>> existingWpcList && existingWpcList.Count > 0)
                            {
                                wpcList = existingWpcList;
                            }
                            
                            if (wpcList != null && wpcList.Count > 0)
                            {
                                if (wpcList[0].TryGetValue("data", out var wpcDataObj))
                                {
                                    Dictionary<string, object>? wpcDataDict = null;
                                    
                                    if (wpcDataObj is JsonElement wpcDataElement)
                                    {
                                        wpcDataDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(wpcDataElement.GetRawText());
                                    }
                                    else if (wpcDataObj is Dictionary<string, object> existingWpcDataDict)
                                    {
                                        wpcDataDict = existingWpcDataDict;
                                    }
                                    
                                    if (wpcDataDict?.TryGetValue("Datasets", out var wpcDatasetsObj) == true)
                                    {
                                        List<string>? wpcDatasetsList = null;
                                        
                                        if (wpcDatasetsObj is JsonElement wpcDatasets && wpcDatasets.ValueKind == JsonValueKind.Array)
                                        {
                                            wpcDatasetsList = System.Text.Json.JsonSerializer.Deserialize<List<string>>(wpcDatasets.GetRawText());
                                        }
                                        else if (wpcDatasetsObj is List<string> existingWpcDatasetsList)
                                        {
                                            wpcDatasetsList = existingWpcDatasetsList;
                                        }
                                        
                                        if (wpcDatasetsList != null && wpcDatasetsList.Count > 0)
                                        {
                                            wpcDatasetsList[0] = $"{fileId}:{fileVersion}";
                                            wpcDataDict["Datasets"] = wpcDatasetsList;
                                            wpcList[0]["data"] = wpcDataDict;
                                        }
                                    }
                                }
                                
                                updatedData["WorkProductComponents"] = wpcList;
                            }
                        }

                        // Generate WorkProduct ID if not present (equivalent to Python's "if id not in data["WorkProduct"]")
                        Dictionary<string, object>? workProductDict = null;
                        
                        if (workProductObj is JsonElement wpElement)
                        {
                            workProductDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(wpElement.GetRawText());
                        }
                        else if (workProductObj is Dictionary<string, object> existingDict)
                        {
                            workProductDict = existingDict;
                        }
                        
                        if (workProductDict != null && !workProductDict.ContainsKey("id"))
                        {
                            var workProductId = GenerateWorkProductId(fileName, baseDir);
                            workProductDict["id"] = workProductId;
                            updatedData["WorkProduct"] = workProductDict;
                        }
                    }
                    else
                    {
                        _logger.LogWarning("File {FileName} does not exist in location map", fileName);
                    }
                }

                _logger.LogDebug("Data to upload workproduct: {Data}", System.Text.Json.JsonSerializer.Serialize(updatedData));
                return updatedData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating work products metadata");
                return data;
            }
        }

        /// <summary>
        /// Updates legal and ACL tags for the specified section
        /// </summary>
        private void UpdateLegalAndAclTags(Dictionary<string, object> data, string sectionName)
        {
            if (data.TryGetValue(sectionName, out var sectionObj))
            {
                Dictionary<string, object>? sectionDict = null;
                
                // Handle both JsonElement and already deserialized object cases
                if (sectionObj is JsonElement section)
                {
                    sectionDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(section.GetRawText());
                }
                else if (sectionObj is Dictionary<string, object> existingSectionDict)
                {
                    sectionDict = existingSectionDict;
                }
                
                if (sectionDict != null)
                {
                    // Update legal tags
                    if (sectionDict.TryGetValue("legal", out var legalObj))
                    {
                        Dictionary<string, object>? legalDict = null;
                        
                        if (legalObj is JsonElement legal)
                        {
                            legalDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(legal.GetRawText());
                        }
                        else if (legalObj is Dictionary<string, object> existingLegalDict)
                        {
                            legalDict = existingLegalDict;
                        }
                        
                        if (legalDict != null)
                        {
                            legalDict["legaltags"] = new[] { _configuration.LegalTag };
                            legalDict["otherRelevantDataCountries"] = new[] { "US" };
                            legalDict["status"] = "compliant";
                            sectionDict["legal"] = legalDict;
                        }
                    }

                    // Update ACL tags
                    if (sectionDict.TryGetValue("acl", out var aclObj))
                    {
                        Dictionary<string, object>? aclDict = null;
                        
                        if (aclObj is JsonElement acl)
                        {
                            aclDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(acl.GetRawText());
                        }
                        else if (aclObj is Dictionary<string, object> existingAclDict)
                        {
                            aclDict = existingAclDict;
                        }
                        
                        if (aclDict != null)
                        {
                            aclDict["viewers"] = new[] { _configuration.AclViewer };
                            aclDict["owners"] = new[] { _configuration.AclOwner };
                            sectionDict["acl"] = aclDict;
                        }
                    }

                    data[sectionName] = sectionDict;
                }
            }
        }

        /// <summary>
        /// Adds metadata to the specified section array
        /// </summary>
        private void AddMetadata(Dictionary<string, object> data, string sectionName)
        {
            if (data.TryGetValue(sectionName, out var sectionObj))
            {
                List<Dictionary<string, object>>? sectionList = null;
                
                // Handle both JsonElement and already deserialized List cases
                if (sectionObj is JsonElement section && section.ValueKind == JsonValueKind.Array)
                {
                    sectionList = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(section.GetRawText());
                }
                else if (sectionObj is List<Dictionary<string, object>> existingList)
                {
                    sectionList = existingList;
                }
                
                if (sectionList != null)
                {
                    foreach (var item in sectionList)
                    {
                        // Create a temporary dictionary to pass to UpdateLegalAndAclTags
                        var itemWrapper = new Dictionary<string, object> { [sectionName.TrimEnd('s')] = item };
                        UpdateLegalAndAclTags(itemWrapper, sectionName.TrimEnd('s'));
                        
                        // Get the updated item back from the wrapper
                        if (itemWrapper.TryGetValue(sectionName.TrimEnd('s'), out var updatedItem) && 
                            updatedItem is Dictionary<string, object> updatedItemDict)
                        {
                            // Copy the updated properties back to the original item
                            foreach (var kvp in updatedItemDict)
                            {
                                item[kvp.Key] = kvp.Value;
                            }
                        }
                    }
                    data[sectionName] = sectionList;
                }
            }
        }

        /// <summary>
        /// Generates a work product ID based on filename and base directory
        /// </summary>
        private string GenerateWorkProductId(string fileName, string baseDir)
        {
            // Equivalent to Python's generate_workproduct_id function
            // Should generate format like: "opendes:work-product--WorkProduct:documents-{fileName}"
            var cleanFileName = fileName.Replace(" ", "_").Replace("-", "_");
            return $"opendes:work-product--WorkProduct:documents-{cleanFileName}";
        }
    }