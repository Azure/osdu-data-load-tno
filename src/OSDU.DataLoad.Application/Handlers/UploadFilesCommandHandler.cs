using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Text.Json;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for uploading files to OSDU following the Python workflow
/// </summary>
public class UploadFilesCommandHandler : IRequestHandler<UploadFilesCommand, LoadResult>
{
    private readonly IOsduClient _osduClient;
    private readonly ILogger<UploadFilesCommandHandler> _logger;

    public UploadFilesCommandHandler(IOsduClient osduClient, ILogger<UploadFilesCommandHandler> logger)
    {
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(UploadFilesCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting file upload for {FileCount} files", request.Files.Count());

        // Authenticate first
        var authenticated = await _osduClient.AuthenticateAsync(cancellationToken);
        if (!authenticated)
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to authenticate with OSDU platform",
                ProcessedRecords = request.Files.Count(),
                FailedRecords = request.Files.Count(),
                Duration = TimeSpan.Zero,
                ErrorDetails = "Authentication failed"
            };
        }

        var startTime = DateTime.UtcNow;
        var successfulUploads = new Dictionary<string, object>();
        var failedUploads = new List<string>();

        // Process files in parallel (similar to Python's ThreadPoolExecutor)
        var uploadTasks = request.Files.Select(async file =>
        {
            try
            {
                var description = Path.GetDirectoryName(file.FilePath)?.Split(Path.DirectorySeparatorChar).LastOrDefault() ?? "unknown";
                var uploadResult = await _osduClient.UploadFileAsync(file.FilePath, file.FileName, description, cancellationToken);
                
                if (uploadResult.IsSuccess)
                {
                    _logger.LogInformation("File upload succeeded: {FileName}", file.FileName);
                    return new { Success = true, FileName = file.FileName, Result = uploadResult };
                }
                else
                {
                    _logger.LogError("File upload failed: {FileName} - {Error}", file.FileName, uploadResult.Message);
                    return new { Success = false, FileName = file.FileName, Result = uploadResult };
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception occurred during file upload: {FileName}", file.FileName);
                return new { Success = false, FileName = file.FileName, Result = new FileUploadResult { IsSuccess = false, Message = ex.Message } };
            }
        });

        var uploadResults = await Task.WhenAll(uploadTasks);

        // Process results
        foreach (var result in uploadResults)
        {
            if (result.Success && result.Result.IsSuccess)
            {
                successfulUploads[result.FileName] = new
                {
                    file_id = result.Result.FileId,
                    file_source = result.Result.FileSource,
                    file_record_version = result.Result.FileRecordVersion,
                    Description = result.Result.Description
                };
            }
            else
            {
                failedUploads.Add(result.FileName);
            }
        }

        // Save successful uploads to JSON file (matching Python behavior)
        if (!string.IsNullOrEmpty(request.OutputPath) && successfulUploads.Any())
        {
            try
            {
                
                Directory.CreateDirectory(Path.GetDirectoryName(request.OutputPath)!);
                
                var jsonOptions = new JsonSerializerOptions { WriteIndented = true };
                var jsonContent = JsonSerializer.Serialize(successfulUploads, jsonOptions);
                await File.WriteAllTextAsync(request.OutputPath, jsonContent, cancellationToken);
                
                _logger.LogInformation("File location map saved to {OutputFile}", request.OutputPath);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to save file location map");
            }
        }

        var duration = DateTime.UtcNow - startTime;

        _logger.LogInformation("Completed file upload. Success: {SuccessCount}, Failed: {FailedCount}", 
            successfulUploads.Count, failedUploads.Count);

        return new LoadResult
        {
            IsSuccess = failedUploads.Count == 0,
            Message = failedUploads.Count == 0 ? "All files uploaded successfully" : $"{failedUploads.Count} files failed to upload",
            ProcessedRecords = request.Files.Count(),
            SuccessfulRecords = successfulUploads.Count,
            FailedRecords = failedUploads.Count,
            Duration = duration,
            ErrorDetails = failedUploads.Any() ? string.Join(", ", failedUploads) : null
        };
    }
}
