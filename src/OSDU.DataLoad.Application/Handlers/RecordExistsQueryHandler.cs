using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Queries;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for checking if records exist
/// </summary>
public class RecordExistsQueryHandler : IRequestHandler<RecordExistsQuery, bool>
{
    private readonly IOsduClient _osduClient;
    private readonly ILogger<RecordExistsQueryHandler> _logger;

    public RecordExistsQueryHandler(IOsduClient osduClient, ILogger<RecordExistsQueryHandler> logger)
    {
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<bool> Handle(RecordExistsQuery request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Checking if record exists: {RecordId}", request.RecordId);

        return await _osduClient.RecordExistsAsync(request.RecordId, cancellationToken);
    }
}
