using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using System.Text.Json;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for processing work product manifest templates and updating them with file location references
/// This matches the Python approach where work product manifests are generated/updated before loading
/// </summary>
public class ProcessWorkProductManifestsCommandHandler : IRequestHandler<ProcessWorkProductManifestsCommand, LoadResult>
{
    private readonly ILogger<ProcessWorkProductManifestsCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;

    public ProcessWorkProductManifestsCommandHandler(
        ILogger<ProcessWorkProductManifestsCommandHandler> logger,
        IOptions<OsduConfiguration> configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(ProcessWorkProductManifestsCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting work product manifest processing");

        try
        {
            var overallResult = new LoadResult
            {
                IsSuccess = true,
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0
            };

            // Process each work product type
            var workProductTypes = new[]
            {
                TnoDataType.Documents,
                TnoDataType.WellLogs,
                TnoDataType.WellMarkers,
                TnoDataType.WellboreTrajectories
            };

            foreach (var dataType in workProductTypes)
            {
                _logger.LogInformation("Processing work product manifests for {DataType}", dataType);

                if (!request.FileLocationMappings.ContainsKey(dataType))
                {
                    _logger.LogWarning("No file location mapping found for {DataType}, skipping", dataType);
                    continue;
                }

                var fileLocationMapPath = request.FileLocationMappings[dataType];
                if (!File.Exists(fileLocationMapPath))
                {
                    _logger.LogWarning("File location mapping file not found: {Path}", fileLocationMapPath);
                    continue;
                }

                var result = await ProcessWorkProductManifestsForType(dataType, fileLocationMapPath, request, cancellationToken);
                
                overallResult.ProcessedRecords += result.ProcessedRecords;
                overallResult.SuccessfulRecords += result.SuccessfulRecords;
                overallResult.FailedRecords += result.FailedRecords;
                overallResult.IsSuccess = overallResult.IsSuccess && result.IsSuccess;
            }

            overallResult.Duration = DateTime.UtcNow - startTime;
            overallResult.Message = $"Processed {overallResult.SuccessfulRecords}/{overallResult.ProcessedRecords} work product manifests";

            return overallResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing work product manifests");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Work product manifest processing failed",
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.Message
            };
        }
    }

    private async Task<LoadResult> ProcessWorkProductManifestsForType(
        TnoDataType dataType, 
        string fileLocationMapPath, 
        ProcessWorkProductManifestsCommand request,
        CancellationToken cancellationToken)
    {
        try
        {
            // Load file location mapping
            var fileLocationMapJson = await File.ReadAllTextAsync(fileLocationMapPath, cancellationToken);
            var fileLocationMap = JsonSerializer.Deserialize<Dictionary<string, object>>(fileLocationMapJson);

            if (fileLocationMap == null)
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = $"Failed to parse file location map for {dataType}",
                    ProcessedRecords = 0,
                    SuccessfulRecords = 0,
                    FailedRecords = 1
                };
            }

            // Find work product template manifests for this data type
            var templateManifestPath = GetTemplateManifestPath(dataType, request.SourceDataPath);
            if (!Directory.Exists(templateManifestPath))
            {
                _logger.LogWarning("Template manifest directory not found: {Path}", templateManifestPath);
                return new LoadResult
                {
                    IsSuccess = true,
                    ProcessedRecords = 0,
                    SuccessfulRecords = 0,
                    FailedRecords = 0
                };
            }

            // Create output directory
            var outputManifestPath = GetOutputManifestPath(dataType, request.OutputPath);
            Directory.CreateDirectory(outputManifestPath);

            // Process template manifests
            var templateFiles = Directory.GetFiles(templateManifestPath, "*.json");
            var processedCount = 0;
            var successCount = 0;

            foreach (var templateFile in templateFiles)
            {
                processedCount++;
                
                try
                {
                    // Load template manifest
                    var templateJson = await File.ReadAllTextAsync(templateFile, cancellationToken);
                    var templateManifest = JsonSerializer.Deserialize<Dictionary<string, object>>(templateJson);

                    if (templateManifest == null)
                    {
                        _logger.LogWarning("Failed to parse template manifest: {TemplateFile}", templateFile);
                        continue;
                    }

                    // Update manifest with file location data (similar to Python's update_work_products_metadata)
                    var updatedManifest = UpdateManifestWithFileLocations(templateManifest, fileLocationMap, dataType);

                    // Save updated manifest
                    var outputFileName = Path.GetFileName(templateFile);
                    var outputFilePath = Path.Combine(outputManifestPath, outputFileName);
                    
                    var updatedJson = JsonSerializer.Serialize(updatedManifest, new JsonSerializerOptions { WriteIndented = true });
                    await File.WriteAllTextAsync(outputFilePath, updatedJson, cancellationToken);

                    successCount++;
                    _logger.LogDebug("Processed work product manifest: {FileName}", outputFileName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing template manifest: {TemplateFile}", templateFile);
                }
            }

            return new LoadResult
            {
                IsSuccess = true,
                ProcessedRecords = processedCount,
                SuccessfulRecords = successCount,
                FailedRecords = processedCount - successCount
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing work product manifests for {DataType}", dataType);
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Failed to process work product manifests for {dataType}: {ex.Message}",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 1
            };
        }
    }

    private string GetTemplateManifestPath(TnoDataType dataType, string sourceDataPath)
    {
        // This should point to where work product template manifests are located
        // Based on the Python implementation, these would be in TNO/provided/TNO/work-products/
        return dataType switch
        {
            TnoDataType.Documents => Path.Combine(sourceDataPath, "TNO/provided/TNO/work-products/documents"),
            TnoDataType.WellLogs => Path.Combine(sourceDataPath, "TNO/provided/TNO/work-products/well logs"),
            TnoDataType.WellMarkers => Path.Combine(sourceDataPath, "TNO/provided/TNO/work-products/markers"),
            TnoDataType.WellboreTrajectories => Path.Combine(sourceDataPath, "TNO/provided/TNO/work-products/trajectories"),
            _ => throw new ArgumentException($"Unsupported work product data type: {dataType}")
        };
    }

    private string GetOutputManifestPath(TnoDataType dataType, string outputPath)
    {
        // Output to the manifests directory structure
        return dataType switch
        {
            TnoDataType.Documents => Path.Combine(outputPath, "manifests/work-product-documents-manifests"),
            TnoDataType.WellLogs => Path.Combine(outputPath, "manifests/work-product-welllogs-manifests"),
            TnoDataType.WellMarkers => Path.Combine(outputPath, "manifests/work-product-markers-manifests"),
            TnoDataType.WellboreTrajectories => Path.Combine(outputPath, "manifests/work-product-trajectories-manifests"),
            _ => throw new ArgumentException($"Unsupported work product data type: {dataType}")
        };
    }

    private Dictionary<string, object> UpdateManifestWithFileLocations(
        Dictionary<string, object> templateManifest, 
        Dictionary<string, object> fileLocationMap, 
        TnoDataType dataType)
    {
        // This method should implement the same logic as Python's update_work_products_metadata
        // For now, return the template as-is - this would need to be implemented based on 
        // the specific structure of your work product manifests and file location maps
        
        _logger.LogWarning("UpdateManifestWithFileLocations not fully implemented - returning template as-is");
        
        // TODO: Implement the file location update logic similar to Python's:
        // 1. Replace namespace placeholders with actual data partition
        // 2. Update file references with actual file IDs from fileLocationMap
        // 3. Update legal tags and ACL information
        
        return templateManifest;
    }
}
