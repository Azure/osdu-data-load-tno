using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading;

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

            // First pass: Discover all files to get total count
            var allDatasetFiles = new Dictionary<string, List<SourceFile>>();
            var totalFileCount = 0;

            _logger.LogInformation("Discovering files across all dataset directories...");
            
            foreach (var datasetDirectory in datasetDirectories)
            {
                var fullDirectoryPath = Path.Combine(request.BasePath, datasetDirectory);
                
                if (Directory.Exists(fullDirectoryPath))
                {
                    var files = DiscoverFilesInDirectory(fullDirectoryPath, datasetDirectory);
                    allDatasetFiles[datasetDirectory] = files;
                    totalFileCount += files.Count;
                    
                    _logger.LogInformation("Dataset {DatasetDirectory}: {FileCount} files", 
                        datasetDirectory, files.Count);
                }
                else
                {
                    allDatasetFiles[datasetDirectory] = new List<SourceFile>();
                    _logger.LogWarning("Dataset directory does not exist: {DirectoryPath}", fullDirectoryPath);
                }
            }

            _logger.LogInformation("Total files discovered across all datasets: {TotalFileCount}", totalFileCount);

            // Initialize overall progress tracking
            var overallProgressTracker = new OverallProgressTracker();
            var failedDatasets = new List<string>();

            // Process each dataset directory with overall progress tracking
            foreach (var datasetDirectory in datasetDirectories)
            {
                _logger.LogInformation("Processing dataset directory: {DatasetDirectory}", datasetDirectory);

                var datasetResult = await ProcessDatasetDirectoryAsync(
                    datasetDirectory,
                    allDatasetFiles[datasetDirectory],
                    request.OutputPath,
                    totalFileCount,
                    overallProgressTracker,
                    cancellationToken);

                if (!datasetResult.IsSuccess)
                {
                    failedDatasets.Add(datasetDirectory);
                }

                _logger.LogInformation("Completed dataset directory: {DatasetDirectory} - Success: {Success}/{Total}, Failed: {Failed}", 
                    datasetDirectory, datasetResult.SuccessfulUploads, datasetResult.ProcessedFiles, datasetResult.FailedUploads);
            }

            var duration = DateTime.UtcNow - startTime;
            var isOverallSuccess = overallProgressTracker.FailedUploads == 0;

            _logger.LogInformation("Dataset upload completed - Total: {Total}, Successful: {Successful}, Failed: {Failed}, Duration: {Duration:mm\\:ss}",
                totalFileCount, overallProgressTracker.SuccessfulUploads, overallProgressTracker.FailedUploads, duration);

            return new LoadResult
            {
                IsSuccess = isOverallSuccess,
                Message = isOverallSuccess 
                    ? $"Successfully uploaded all {overallProgressTracker.SuccessfulUploads} files across {datasetDirectories.Count} dataset directories"
                    : $"Uploaded {overallProgressTracker.SuccessfulUploads} of {totalFileCount} files. Failed datasets: {string.Join(", ", failedDatasets)}",
                ProcessedRecords = totalFileCount,
                SuccessfulRecords = overallProgressTracker.SuccessfulUploads,
                FailedRecords = overallProgressTracker.FailedUploads,
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
        List<SourceFile> files,
        string outputPath,
        int totalFilesAcrossAllDatasets,
        OverallProgressTracker overallProgressTracker,
        CancellationToken cancellationToken)
    {
        var datasetStartTime = DateTime.UtcNow;
        var datasetSuccessfulUploads = 0;
        var datasetFailedUploads = 0;
        var datasetProcessedFiles = 0;
        var fileLocationMappings = new ConcurrentDictionary<string, object>();
        var failedFiles = new ConcurrentBag<string>();

        _logger.LogInformation("Starting upload for dataset directory: {DatasetDirectory}", datasetDirectory);

        try
        {
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

            _logger.LogInformation("Processing {FileCount} files in dataset directory: {DatasetDirectory}", files.Count, datasetDirectory);

            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = Environment.ProcessorCount * 8,
                CancellationToken = cancellationToken
            };

            await Parallel.ForEachAsync(files, options, async (file, ct) =>
            {
                var datasetCurrent = Interlocked.Increment(ref datasetProcessedFiles);
                var overallCurrent = overallProgressTracker.IncrementProcessed();
                
                var datasetProgress = (double)datasetCurrent / files.Count * 100;
                var overallProgress = (double)overallCurrent / totalFilesAcrossAllDatasets * 100;

                try
                {
                    var result = await _osduService.UploadFileAsync(file, ct);

                    if (result.IsSuccess)
                    {
                        Interlocked.Increment(ref datasetSuccessfulUploads);
                        overallProgressTracker.IncrementSuccessful();
                        
                        fileLocationMappings[file.FileName] = new
                        {
                            FileId = result.FileId,
                            Version = result.FileRecordVersion,
                            OriginalPath = file.FilePath,
                            DatasetDirectory = datasetDirectory,
                            UploadedAt = DateTime.UtcNow
                        };

                        _logger.LogInformation("✓ Overall Progress: [{OverallCurrent}/{OverallTotal}] ({OverallProgress:F1}%) | Dataset [{DatasetDirectory}]: [{DatasetCurrent}/{DatasetTotal}] ({DatasetProgress:F1}%) | Uploaded: {FileName} -> {FileId}:{Version}",
                            overallCurrent, totalFilesAcrossAllDatasets, overallProgress,
                            datasetDirectory, datasetCurrent, files.Count, datasetProgress,
                            file.FileName, result.FileId, result.FileRecordVersion);
                    }
                    else
                    {
                        Interlocked.Increment(ref datasetFailedUploads);
                        overallProgressTracker.IncrementFailed();
                        failedFiles.Add(file.FileName);

                        _logger.LogError("✗ Overall Progress: [{OverallCurrent}/{OverallTotal}] ({OverallProgress:F1}%) | Dataset [{DatasetDirectory}]: [{DatasetCurrent}/{DatasetTotal}] ({DatasetProgress:F1}%) | Failed: {FileName} - {Error}",
                            overallCurrent, totalFilesAcrossAllDatasets, overallProgress,
                            datasetDirectory, datasetCurrent, files.Count, datasetProgress,
                            file.FileName, result.Message);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref datasetFailedUploads);
                    overallProgressTracker.IncrementFailed();
                    failedFiles.Add(file.FileName);

                    _logger.LogError(ex, "✗ Overall Progress: [{OverallCurrent}/{OverallTotal}] ({OverallProgress:F1}%) | Dataset [{DatasetDirectory}]: [{DatasetCurrent}/{DatasetTotal}] ({DatasetProgress:F1}%) | Exception: {FileName}",
                        overallCurrent, totalFilesAcrossAllDatasets, overallProgress,
                        datasetDirectory, datasetCurrent, files.Count, datasetProgress,
                        file.FileName);
                }
            });

            // Save dataset-specific output file
            if (fileLocationMappings.Any())
            {
                await SaveDatasetOutputFileAsync(datasetDirectory, new Dictionary<string, object>(fileLocationMappings), outputPath, cancellationToken);
            }

            var duration = DateTime.UtcNow - datasetStartTime;
            var isSuccess = datasetFailedUploads == 0;

            _logger.LogInformation("Completed dataset {DatasetDirectory}: {Successful}/{Total} successful, {Failed} failed, Duration: {Duration:mm\\:ss}",
                datasetDirectory, datasetSuccessfulUploads, files.Count, datasetFailedUploads, duration);

            return new DatasetUploadResult
            {
                IsSuccess = isSuccess,
                DatasetType = datasetDirectory,
                ProcessedFiles = datasetProcessedFiles,
                SuccessfulUploads = datasetSuccessfulUploads,
                FailedUploads = datasetFailedUploads,
                Duration = duration,
                FailedFiles = failedFiles.ToList()
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
                ProcessedFiles = datasetProcessedFiles,
                SuccessfulUploads = datasetSuccessfulUploads,
                FailedUploads = int.MaxValue, // Unknown total, so mark as failed
                Duration = duration,
                FailedFiles = failedFiles.ToList()
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
/// Thread-safe tracker for overall progress across all datasets
/// </summary>
internal class OverallProgressTracker
{
    private int _processedFiles = 0;
    private int _successfulUploads = 0;
    private int _failedUploads = 0;

    public int ProcessedFiles => _processedFiles;
    public int SuccessfulUploads => _successfulUploads;
    public int FailedUploads => _failedUploads;

    public int IncrementProcessed() => Interlocked.Increment(ref _processedFiles);
    public int IncrementSuccessful() => Interlocked.Increment(ref _successfulUploads);
    public int IncrementFailed() => Interlocked.Increment(ref _failedUploads);
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