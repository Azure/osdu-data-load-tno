using MediatR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Model for file information in the file location mapping
/// </summary>
public class FileInfo
{
    public string? FileId { get; set; }
    public string? Version { get; set; }
    public string? OriginalPath { get; set; }
    public string? DatasetDirectory { get; set; }
    public DateTime? UploadedAt { get; set; }
}

/// <summary>
/// Model for the file location mapping structure
/// </summary>
public class FileLocationMapping
{
    public string? DatasetDirectory { get; set; }
    public DateTime? GeneratedAt { get; set; }
    public int TotalFiles { get; set; }
    public Dictionary<string, FileInfo>? Files { get; set; }
}

/// <summary>
/// Handler for generating work product manifests
/// </summary>
public class GenerateWorkProductManifestCommandHandler : IRequestHandler<GenerateWorkProductManifestCommand, LoadResult>
{
    private readonly IManifestGenerator _manifestGenerator;
    private readonly ILogger<GenerateWorkProductManifestCommandHandler> _logger;
    private readonly PathConfiguration _pathConfig;

    public GenerateWorkProductManifestCommandHandler(
        IManifestGenerator manifestGenerator,
        ILogger<GenerateWorkProductManifestCommandHandler> logger,
        PathConfiguration pathConfig)
    {
        _manifestGenerator = manifestGenerator ?? throw new ArgumentNullException(nameof(manifestGenerator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _pathConfig = pathConfig ?? throw new ArgumentNullException(nameof(pathConfig));
    }

    public async Task<LoadResult> Handle(GenerateWorkProductManifestCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting work product manifest generation from {SourceDataPath}", request.SourceDataPath);

        try
        {
            // Validate inputs
            if (string.IsNullOrWhiteSpace(request.SourceDataPath))
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = "Source data path is required",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            if (!Directory.Exists(request.SourceDataPath))
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = $"Source directory does not exist: {request.SourceDataPath}",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            // Ensure output directory exists
            Directory.CreateDirectory(_pathConfig.WorkProductManifestsPath);

            // Generate work product manifests using existing manifest generator
            // This includes documents, well logs, markers, and trajectories
            var workProductTypes = new[]
            {
                TnoDataType.Documents,
                TnoDataType.WellLogs,
                TnoDataType.WellMarkers,
                TnoDataType.WellboreTrajectories
            };

            var overallResult = new LoadResult
            {
                IsSuccess = true,
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0
            };

            foreach (var manifestConfig in request.ManifestConfigs)
            {
                _logger.LogInformation("Loading {DataType} work products", manifestConfig.Type);

                // Check if the subdirectory exists for this data type
                var subdirectory = manifestConfig.DataDir;
                var dataTypePath = Path.Combine(request.SourceDataPath, subdirectory);

                if (!Directory.Exists(dataTypePath))
                {
                    _logger.LogWarning("Skipping {DataType} - directory not found: {Path}", manifestConfig.Type, dataTypePath);
                    continue;
                }
                var fileLocationMap = Path.Combine(request.WorkProductsMappingPath, manifestConfig.MappingFile);
                
                if (string.IsNullOrEmpty(fileLocationMap) || !File.Exists(fileLocationMap))
                {
                    _logger.LogWarning("Skipping {DataType} - file location mapping not found: {Map}", manifestConfig.Type, fileLocationMap);
                    continue;
                }

                // loop json dir✅
                // loop through files
                var manifestFiles = Directory.GetFiles(dataTypePath, "*.json", SearchOption.AllDirectories);
                foreach (var manifestFile in manifestFiles)
                {
                    // get data property
                    var manifestJson = await File.ReadAllTextAsync(manifestFile, cancellationToken);
                    var manifestObject = JsonSerializer.Deserialize<Dictionary<string, object>>(manifestJson);

                    if (manifestObject == null)
                    {
                        _logger.LogWarning("Failed to parse manifest file during pre-scan: {ManifestFile}", Path.GetFileName(manifestFile));
                        continue;
                    }

                    var data = manifestObject["Data"];
                    var jsonBefore = JsonSerializer.Serialize(data);
                    Console.WriteLine(jsonBefore);
                    // Update work product data (equivalent to Python's update_work_products_metadata)
                    var updatedData = await UpdateWorkProductsMetadata(data, fileLocationMap, request.SourceDataPath, request.DataPartition, request.LegalTag, request.AclViewer, request.AclOwner);

                    var jsonAfter = JsonSerializer.Serialize(updatedData);
                    Console.WriteLine(jsonAfter);

                    var manifest = new Dictionary<string, object>
                    {
                        ["Data"] = updatedData
                    };

                    var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
                    var updatedManifestJson = JsonSerializer.Serialize(manifest, jsonOptions);

                    var workProductsDir = Path.Combine(request.SourceDataPath, "manifests", manifestConfig.OutputDir);
                    Directory.CreateDirectory(workProductsDir);

                    var manifestFileName = Path.GetFileNameWithoutExtension(manifestFile);
                    var outputFileName = $"{manifestFileName}_ingest.json";
                    var outputFilePath = Path.Combine(workProductsDir, outputFileName);

                    await File.WriteAllTextAsync(outputFilePath, updatedManifestJson, cancellationToken);
                    _logger.LogInformation("Wrote work product manifest to: {OutputPath}", outputFilePath);
                }
            }

            overallResult.Duration = DateTime.UtcNow - startTime;
            overallResult.Message = overallResult.IsSuccess 
                ? $"Work product manifests generated successfully. Processed: {overallResult.ProcessedRecords}, Successful: {overallResult.SuccessfulRecords}"
                : overallResult.Message;

            _logger.LogInformation("Work product manifest generation completed in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} successful",
                overallResult.Duration, overallResult.SuccessfulRecords, overallResult.ProcessedRecords);

            return overallResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during work product manifest generation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Work product manifest generation failed: {ex.Message}",
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.ToString()
            };
        }
    }

    private async Task<object> UpdateWorkProductsMetadata(object data, string fileLocationMapPath, string baseDir, string dataPartition, string legalTag, string aclViewer, string aclOwner)
    {
        try
        {
            // Create namespace patterns (equivalent to Python's reference_pattern and master_pattern)
            var referencePattern = $"{dataPartition}:reference-data";
            var masterPattern = $"{dataPartition}:master-data";

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
            UpdateLegalAndAclTags(updatedData, "WorkProduct", legalTag, aclViewer, aclOwner);
            AddMetadata(updatedData, "WorkProductComponents", legalTag, aclViewer, aclOwner);
            AddMetadata(updatedData, "Datasets", legalTag, aclViewer, aclOwner);

            // Load file location map (equivalent to Python's "with open(file_location_map) as file")
            if (!File.Exists(fileLocationMapPath))
            {
                _logger.LogWarning("File location map not found: {Path}", fileLocationMapPath);
                return updatedData;
            }

            var locationMapJson = await File.ReadAllTextAsync(fileLocationMapPath);
            
            // Parse the file location mapping structure
            Dictionary<string, Dictionary<string, object>>? locationMap = null;
            try
            {
                // Deserialize as the new structure with Files property
                var fileLocationMapping = System.Text.Json.JsonSerializer.Deserialize<FileLocationMapping>(locationMapJson);
                if (fileLocationMapping?.Files != null)
                {
                    // Convert FileInfo objects to Dictionary<string, object> for compatibility
                    locationMap = fileLocationMapping.Files.ToDictionary(
                        kvp => kvp.Key,
                        kvp => new Dictionary<string, object>
                        {
                            ["file_source"] = kvp.Value.FileId ?? "",
                            ["file_id"] = kvp.Value.FileId ?? "",
                            ["file_record_version"] = kvp.Value.Version ?? ""
                        }
                    );
                }
                else
                {
                    _logger.LogWarning("File location mapping does not contain Files property");
                    return updatedData;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to parse file location map");
                return updatedData;
            }

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
    private void UpdateLegalAndAclTags(Dictionary<string, object> data, string sectionName, string legalTag, string aclViewer, string aclOwner)
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
                        legalDict["legaltags"] = new[] { legalTag };
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
                        aclDict["viewers"] = new[] { aclViewer };
                        aclDict["owners"] = new[] { aclOwner};
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
    private void AddMetadata(Dictionary<string, object> data, string sectionName, string legalTag, string aclViewer, string aclOwner)
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
                    UpdateLegalAndAclTags(itemWrapper, sectionName.TrimEnd('s'), legalTag, aclViewer, aclOwner);

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
