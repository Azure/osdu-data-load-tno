using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for uploading records to OSDU
/// </summary>
public class UploadRecordsCommandHandler : IRequestHandler<UploadRecordsCommand, LoadResult>
{
    private readonly IOsduClient _osduClient;
    private readonly ILogger<UploadRecordsCommandHandler> _logger;

    public UploadRecordsCommandHandler(IOsduClient osduClient, ILogger<UploadRecordsCommandHandler> logger)
    {
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(UploadRecordsCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Uploading {RecordCount} records to OSDU", 
            request.Records.Count());

        // Authenticate first
        var authenticated = await _osduClient.AuthenticateAsync(cancellationToken);
        if (!authenticated)
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to authenticate with OSDU platform",
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = request.Records.Count(),
                Duration = TimeSpan.Zero,
                ErrorDetails = "Authentication failed"
            };
        }

        // Upload records
        return await _osduClient.UploadRecordsAsync(request.Records, cancellationToken);
    }
}
