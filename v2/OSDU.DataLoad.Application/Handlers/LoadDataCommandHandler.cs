using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Application.Queries;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for loading data from source path
/// </summary>
public class LoadDataCommandHandler : IRequestHandler<LoadDataCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<LoadDataCommandHandler> _logger;

    public LoadDataCommandHandler(IMediator mediator, ILogger<LoadDataCommandHandler> logger)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(LoadDataCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting data load operation for {DataType} from {SourcePath}", 
            request.DataType, request.SourcePath);

        var startTime = DateTime.UtcNow;

        try
        {
            // Step 1: Discover files
            var sourceFiles = await _mediator.Send(new DiscoverFilesQuery 
            { 
                DirectoryPath = request.SourcePath, 
                DataType = request.DataType 
            }, cancellationToken);

            if (sourceFiles.Length == 0)
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = $"No files found for {request.DataType} data in {request.SourcePath}",
                    ProcessedRecords = 0,
                    SuccessfulRecords = 0,
                    FailedRecords = 0,
                    Duration = DateTime.UtcNow - startTime
                };
            }

            // Step 2: Generate manifest
            var manifest = await _mediator.Send(new GenerateManifestCommand
            {
                SourceFiles = sourceFiles,
                DataType = request.DataType
            }, cancellationToken);

            // Step 3: Load from manifest
            return await _mediator.Send(new LoadFromManifestCommand 
            { 
                Manifest = manifest 
            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during data load operation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Data load operation failed",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.Message
            };
        }
    }
}
