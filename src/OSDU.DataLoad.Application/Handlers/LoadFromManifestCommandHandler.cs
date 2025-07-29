using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for loading data from manifest
/// </summary>
public class LoadFromManifestCommandHandler : IRequestHandler<LoadFromManifestCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<LoadFromManifestCommandHandler> _logger;

    public LoadFromManifestCommandHandler(IMediator mediator, ILogger<LoadFromManifestCommandHandler> logger)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(LoadFromManifestCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting data load from manifest with {FileCount} files", 
            request.Manifest.SourceFiles.Length);

        var startTime = DateTime.UtcNow;
        var allRecords = new List<DataRecord>();
        var fileLocationMap = new Dictionary<string, object>();

        try
        {
            // Determine the data type from the manifest kind
            if (!Enum.TryParse<TnoDataType>(request.Manifest.Kind, true, out var dataType))
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = $"Invalid data type in manifest: {request.Manifest.Kind}",
                    ProcessedRecords = 0,
                    SuccessfulRecords = 0,
                    FailedRecords = 0,
                    Duration = DateTime.UtcNow - startTime,
                    ErrorDetails = $"Unable to parse data type: {request.Manifest.Kind}"
                };
            }

            // Step 1: Handle file uploads for file-based data types
            if (RequiresFileUpload(dataType))
            {
                _logger.LogInformation("Data type {DataType} requires file upload, uploading files first", dataType);
                
                var fileUploadResult = await _mediator.Send(new UploadFilesCommand(request.Manifest.SourceFiles, ""), cancellationToken);
                
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
            foreach (var sourceFile in request.Manifest.SourceFiles)
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
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during manifest load operation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Manifest load operation failed",
                ProcessedRecords = allRecords.Count,
                SuccessfulRecords = 0,
                FailedRecords = allRecords.Count,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.Message
            };
        }
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
}
