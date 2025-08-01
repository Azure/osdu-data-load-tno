using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Text.Json;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for uploading files from configured dataset directories to OSDU
/// </summary>
public class UploadFilesCommandHandler : IRequestHandler<UploadFilesCommand, LoadResult>
{
    private readonly IOsduService _osduService;
    private readonly ILogger<UploadFilesCommandHandler> _logger;
    private readonly PathConfiguration _pathConfig;

    public UploadFilesCommandHandler(
        IOsduService osduService,
        ILogger<UploadFilesCommandHandler> logger,
        PathConfiguration pathConfig)
    {
        _osduService = osduService ?? throw new ArgumentNullException(nameof(osduService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _pathConfig = pathConfig ?? throw new ArgumentNullException(nameof(pathConfig));
    }

    public async Task<LoadResult> Handle(UploadFilesCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting dataset files upload from configured directories");

        try
        {
            // Ensure output directory exists
            Directory.CreateDirectory(request.OutputPath);

            // Get configured dataset directories
            var datasetDirectories = DatasetConfiguration.GetDatasetDirectories().ToList();
            _logger.LogInformation("Processing {DatasetCount} configured dataset directories", datasetDirectories.Count);

            var overallSuccessfulUploads = 0;
            var overallFailedUploads = 0;
            var overallProcessedFiles = 0;
            var failedDatasets = new List<string>();

            // Process each dataset directory
            foreach (var datasetDirectory in datasetDirectories)
            {
                _logger.LogInformation("Processing dataset directory: {DatasetDirectory}", datasetDirectory);

                var datasetResult = await ProcessDatasetDirectoryAsync(
                    datasetDirectory, 
                    request.BasePath,
                    request.OutputPath, 
                    cancellationToken);

                overallSuccessfulUploads += datasetResult.SuccessfulUploads;
                overallFailedUploads += datasetResult.FailedUploads;
                overallProcessedFiles += datasetResult.ProcessedFiles;

                if (!datasetResult.IsSuccess)
                {
                    failedDatasets.Add(datasetDirectory);
                }

                _logger.LogInformation("Completed dataset directory: {DatasetDirectory} - Success: {Success}/{Total}, Failed: {Failed}", 
                    datasetDirectory, datasetResult.SuccessfulUploads, datasetResult.ProcessedFiles, datasetResult.FailedUploads);
            }

            var duration = DateTime.UtcNow - startTime;
            var isOverallSuccess = overallFailedUploads == 0;

            _logger.LogInformation("Dataset upload completed - Total: {Total}, Successful: {Successful}, Failed: {Failed}, Duration: {Duration:mm\\:ss}",
                overallProcessedFiles, overallSuccessfulUploads, overallFailedUploads, duration);

            return new LoadResult
            {
                IsSuccess = isOverallSuccess,
                Message = isOverallSuccess 
                    ? $"Successfully uploaded all {overallSuccessfulUploads} files across {datasetDirectories.Count} dataset directories"
                    : $"Uploaded {overallSuccessfulUploads} of {overallProcessedFiles} files. Failed datasets: {string.Join(", ", failedDatasets)}",
                ProcessedRecords = overallProcessedFiles,
                SuccessfulRecords = overallSuccessfulUploads,
                FailedRecords = overallFailedUploads,
                Duration = duration,
                ErrorDetails = failedDatasets.Any() 
                    ? $"Failed dataset directories: {string.Join(", ", failedDatasets)}"
                    : string.Empty
            };
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - startTime;
            _logger.LogError(ex, "Error during dataset files upload");
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Dataset files upload failed: {ex.Message}",
                Duration = duration,
                ErrorDetails = ex.ToString()
            };
        }
    }

    /// <summary>
    /// Processes files from a specific dataset directory and creates the corresponding output file
    /// </summary>
    private async Task<DatasetUploadResult> ProcessDatasetDirectoryAsync(
        string datasetDirectory,
        string basePath,
        string outputPath, 
        CancellationToken cancellationToken)
    {
        var datasetStartTime = DateTime.UtcNow;
        var successfulUploads = 0;
        var failedUploads = 0;
        var processedFiles = 0;
        var fileLocationMappings = new Dictionary<string, object>();
        var failedFiles = new List<string>();

        _logger.LogInformation("Starting upload for dataset directory: {DatasetDirectory}", datasetDirectory);

        try
        {
            // Build full directory path
            var fullDirectoryPath = Path.Combine(basePath, datasetDirectory);
            
            if (!Directory.Exists(fullDirectoryPath))
            {
                _logger.LogWarning("Dataset directory does not exist: {DirectoryPath}", fullDirectoryPath);
                return new DatasetUploadResult
                {
                    IsSuccess = true, // Empty directory is not a failure
                    DatasetType = datasetDirectory,
                    ProcessedFiles = 0,
                    SuccessfulUploads = 0,
                    FailedUploads = 0,
                    Duration = DateTime.UtcNow - datasetStartTime,
                    FailedFiles = new List<string>()
                };
            }

            // Discover files in the directory
            var files = DiscoverFilesInDirectory(fullDirectoryPath, datasetDirectory);
            
            if (!files.Any())
            {
                _logger.LogInformation("No files found in dataset directory: {DatasetDirectory}", datasetDirectory);
                return new DatasetUploadResult
                {
                    IsSuccess = true,
                    DatasetType = datasetDirectory,
                    ProcessedFiles = 0,
                    SuccessfulUploads = 0,
                    FailedUploads = 0,
                    Duration = DateTime.UtcNow - datasetStartTime,
                    FailedFiles = new List<string>()
                };
            }

            _logger.LogInformation("Found {FileCount} files in dataset directory: {DatasetDirectory}", files.Count, datasetDirectory);

            // Process each file in the dataset directory
            foreach (var file in files)
            {
                processedFiles++;
                var datasetProgress = (double)processedFiles / files.Count * 100;
                
                try
                {
                    _logger.LogInformation("Uploading [{Current}/{Total}] ({Progress:F1}%) - {DatasetDirectory}: {FileName}", 
                        processedFiles, files.Count, datasetProgress, datasetDirectory, file.FileName);
                    
                    var uploadResult = await _osduService.UploadFileAsync(file, cancellationToken);

                    if (uploadResult.IsSuccess)
                    {
                        successfulUploads++;
                        // Store file location mapping for this dataset
                        fileLocationMappings[file.FileName] = new
                        {
                            FileId = uploadResult.FileId,
                            Version = uploadResult.FileRecordVersion,
                            OriginalPath = file.FilePath,
                            DatasetDirectory = datasetDirectory,
                            UploadedAt = DateTime.UtcNow
                        };
                        _logger.LogInformation("✓ Successfully uploaded [{Current}/{Total}]: {FileName} -> {FileId}:{Version}", 
                            processedFiles, files.Count, file.FileName, uploadResult.FileId, uploadResult.FileRecordVersion);
                    }
                    else
                    {
                        failedUploads++;
                        failedFiles.Add(file.FileName);
                        _logger.LogError("✗ Failed to upload [{Current}/{Total}]: {FileName} - {Error}", 
                            processedFiles, files.Count, file.FileName, uploadResult.Message);
                    }
                }
                catch (Exception ex)
                {
                    failedUploads++;
                    failedFiles.Add(file.FileName);
                    _logger.LogError(ex, "✗ Exception while uploading [{Current}/{Total}]: {FileName}", 
                        processedFiles, files.Count, file.FileName);
                }
            }

            // Save dataset-specific output file
            if (fileLocationMappings.Any())
            {
                await SaveDatasetOutputFileAsync(datasetDirectory, fileLocationMappings, outputPath, cancellationToken);
            }

            var duration = DateTime.UtcNow - datasetStartTime;
            var isSuccess = failedUploads == 0;

            _logger.LogInformation("Completed dataset {DatasetDirectory}: {Successful}/{Total} successful, {Failed} failed, Duration: {Duration:mm\\:ss}",
                datasetDirectory, successfulUploads, files.Count, failedUploads, duration);

            return new DatasetUploadResult
            {
                IsSuccess = isSuccess,
                DatasetType = datasetDirectory,
                ProcessedFiles = processedFiles,
                SuccessfulUploads = successfulUploads,
                FailedUploads = failedUploads,
                Duration = duration,
                FailedFiles = failedFiles
            };
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - datasetStartTime;
            _logger.LogError(ex, "Error processing dataset directory: {DatasetDirectory}", datasetDirectory);
            
            return new DatasetUploadResult
            {
                IsSuccess = false,
                DatasetType = datasetDirectory,
                ProcessedFiles = processedFiles,
                SuccessfulUploads = successfulUploads,
                FailedUploads = int.MaxValue, // Unknown total, so mark as failed
                Duration = duration,
                FailedFiles = failedFiles
            };
        }
    }

    /// <summary>
    /// Discovers all files in a dataset directory and creates SourceFile objects
    /// </summary>
    private List<SourceFile> DiscoverFilesInDirectory(string directoryPath, string datasetDirectory)
    {
        var files = new List<SourceFile>();

        try
        {
            // Get all files recursively from the directory
            var filePaths = Directory.GetFiles(directoryPath, "*", SearchOption.AllDirectories);
            
            foreach (var filePath in filePaths)
            {
                var fileName = Path.GetFileName(filePath);
                
                // Skip hidden files and system files
                if (fileName.StartsWith(".") || fileName.StartsWith("~"))
                    continue;
                
                var sourceFile = new SourceFile
                {
                    FileName = fileName,
                    FilePath = filePath
                };
                
                files.Add(sourceFile);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error discovering files in directory: {DirectoryPath}", directoryPath);
        }

        return files;
    }

    /// <summary>
    /// Saves the output file for a specific dataset directory as uploads progress
    /// </summary>
    private async Task SaveDatasetOutputFileAsync(
        string datasetDirectory, 
        Dictionary<string, object> fileLocationMappings, 
        string outputPath,
        CancellationToken cancellationToken)
    {
        try
        {
            var outputFileName = DatasetConfiguration.GetOutputFileName(datasetDirectory) ?? $"loaded-{datasetDirectory.Replace("/", "-")}-datasets.json";
            var outputFilePath = Path.Combine(outputPath, outputFileName);
            
            // Ensure output directory exists
            Directory.CreateDirectory(Path.GetDirectoryName(outputFilePath)!);
            
            var outputData = new
            {
                DatasetDirectory = datasetDirectory,
                GeneratedAt = DateTime.UtcNow,
                TotalFiles = fileLocationMappings.Count,
                Files = fileLocationMappings
            };

            var json = JsonSerializer.Serialize(outputData, new JsonSerializerOptions 
            { 
                WriteIndented = true 
            });
            
            await File.WriteAllTextAsync(outputFilePath, json, cancellationToken);
            
            _logger.LogInformation("Dataset output file saved: {OutputFile} ({FileCount} files)", 
                outputFilePath, fileLocationMappings.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save dataset output file for {DatasetDirectory}", datasetDirectory);
        }
    }
}

/// <summary>
/// Result of processing a specific dataset directory
/// </summary>
internal class DatasetUploadResult
{
    public bool IsSuccess { get; init; }
    public string DatasetType { get; init; } = string.Empty;
    public int ProcessedFiles { get; init; }
    public int SuccessfulUploads { get; init; }
    public int FailedUploads { get; init; }
    public TimeSpan Duration { get; init; }
    public List<string> FailedFiles { get; init; } = new();
}
