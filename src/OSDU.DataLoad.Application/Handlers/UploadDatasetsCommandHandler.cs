using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using System.Text.Json;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for uploading dataset files to OSDU and creating location mappings
/// This corresponds to the "LoadFiles" step in the Python solution
/// </summary>
public class UploadDatasetsCommandHandler : IRequestHandler<UploadDatasetsCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<UploadDatasetsCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;

    public UploadDatasetsCommandHandler(
        IMediator mediator,
        ILogger<UploadDatasetsCommandHandler> logger,
        IOptions<OsduConfiguration> configuration)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(UploadDatasetsCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting dataset files upload from {SourcePath}", request.SourceDataPath);

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

        var outputDir = string.IsNullOrWhiteSpace(request.OutputPath) 
            ? Path.Combine(request.SourceDataPath, "output") 
            : request.OutputPath;
        
        Directory.CreateDirectory(outputDir);

        var overallResult = new LoadResult
        {
            IsSuccess = true,
            ProcessedRecords = 0,
            SuccessfulRecords = 0,
            FailedRecords = 0
        };

        try
        {
            // Define dataset directories to upload (matching Python solution)
            var datasetDirectories = new Dictionary<string, string>
            {
                { "datasets/documents", "loaded-documents-datasets.json" },
                { "datasets/well-logs", "loaded-welllogs-datasets.json" },
                { "datasets/markers", "loaded-marker-datasets.json" },
                { "datasets/trajectories", "loaded-trajectories-datasets.json" }
            };

            // Upload each dataset directory
            foreach (var (datasetDir, outputFileName) in datasetDirectories)
            {
                var datasetPath = Path.Combine(request.SourceDataPath, datasetDir);
                if (!Directory.Exists(datasetPath))
                {
                    _logger.LogWarning("Skipping {DatasetDir} - directory not found: {Path}", datasetDir, datasetPath);
                    continue;
                }

                _logger.LogInformation("Uploading dataset files from {DatasetPath}", datasetPath);
                var phaseStartTime = DateTime.UtcNow;

                // Find all files in the directory
                var files = Directory.GetFiles(datasetPath, "*", SearchOption.AllDirectories)
                    .Where(f => IsUploadableFile(f))
                    .Select(f => new SourceFile
                    {
                        FilePath = f,
                        FileName = Path.GetFileName(f),
                        FileType = Path.GetExtension(f),
                        Size = new FileInfo(f).Length,
                        LastModified = File.GetLastWriteTime(f)
                    });

                if (!files.Any())
                {
                    _logger.LogWarning("No uploadable files found in {DatasetPath}", datasetPath);
                    continue;
                }

                // Upload files and get location mappings
                var uploadResult = await _mediator.Send(new UploadFilesCommand(files, Path.Combine(outputDir, outputFileName)), cancellationToken);

                // Check if upload failed - fail fast
                if (!uploadResult.IsSuccess)
                {
                    _logger.LogError("Dataset upload failed for {DatasetDir}: {Message}", datasetDir, uploadResult.Message);
                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = $"Dataset upload failed for {datasetDir}: {uploadResult.Message}",
                        Duration = DateTime.UtcNow - startTime,
                        ErrorDetails = uploadResult.ErrorDetails,
                        ProcessedRecords = overallResult.ProcessedRecords + uploadResult.ProcessedRecords,
                        SuccessfulRecords = overallResult.SuccessfulRecords + uploadResult.SuccessfulRecords,
                        FailedRecords = overallResult.FailedRecords + uploadResult.FailedRecords
                    };
                }

                // Aggregate results
                overallResult = new LoadResult
                {
                    IsSuccess = overallResult.IsSuccess && uploadResult.IsSuccess,
                    ProcessedRecords = overallResult.ProcessedRecords + uploadResult.ProcessedRecords,
                    SuccessfulRecords = overallResult.SuccessfulRecords + uploadResult.SuccessfulRecords,
                    FailedRecords = overallResult.FailedRecords + uploadResult.FailedRecords,
                    Duration = DateTime.UtcNow - startTime
                };

                var phaseTime = DateTime.UtcNow - phaseStartTime;
                _logger.LogInformation("Completed {DatasetDir} upload in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} files successful",
                    datasetDir, phaseTime, uploadResult.SuccessfulRecords, uploadResult.ProcessedRecords);
            }

            overallResult = new LoadResult
            {
                IsSuccess = overallResult.IsSuccess,
                ProcessedRecords = overallResult.ProcessedRecords,
                SuccessfulRecords = overallResult.SuccessfulRecords,
                FailedRecords = overallResult.FailedRecords,
                Message = $"Dataset upload completed - {overallResult.SuccessfulRecords}/{overallResult.ProcessedRecords} files successful",
                Duration = DateTime.UtcNow - startTime
            };

            _logger.LogInformation("Completed dataset files upload in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} total files successful",
                overallResult.Duration, overallResult.SuccessfulRecords, overallResult.ProcessedRecords);

            return overallResult;
        }
        catch (OperationCanceledException ex)
        {
            _logger.LogWarning("Dataset files upload was cancelled");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Dataset files upload was cancelled",
                ErrorDetails = "Operation was cancelled",
                Duration = DateTime.UtcNow - startTime
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during dataset files upload");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to upload dataset files",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }

    private static bool IsUploadableFile(string filePath)
    {
        var extensions = new[] { ".pdf", ".csv", ".las", ".txt" };
        var extension = Path.GetExtension(filePath).ToLowerInvariant();
        return extensions.Contains(extension);
    }
}
