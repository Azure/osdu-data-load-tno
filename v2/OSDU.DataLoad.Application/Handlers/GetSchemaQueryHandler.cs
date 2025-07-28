using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Queries;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for getting schema information
/// </summary>
public class GetSchemaQueryHandler : IRequestHandler<GetSchemaQuery, string>
{
    private readonly IOsduClient _osduClient;
    private readonly ILogger<GetSchemaQueryHandler> _logger;

    public GetSchemaQueryHandler(IOsduClient osduClient, ILogger<GetSchemaQueryHandler> logger)
    {
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<string> Handle(GetSchemaQuery request, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Getting schema for kind: {Kind}", request.Kind);

        return await _osduClient.GetSchemaAsync(request.Kind, cancellationToken);
    }
}
