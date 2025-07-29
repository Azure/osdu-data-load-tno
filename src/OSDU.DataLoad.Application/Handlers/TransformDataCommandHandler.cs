using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for transforming data
/// </summary>
public class TransformDataCommandHandler : IRequestHandler<TransformDataCommand, DataRecord[]>
{
    private readonly IDataTransformer _dataTransformer;
    private readonly ILogger<TransformDataCommandHandler> _logger;

    public TransformDataCommandHandler(IDataTransformer dataTransformer, ILogger<TransformDataCommandHandler> logger)
    {
        _dataTransformer = dataTransformer ?? throw new ArgumentNullException(nameof(dataTransformer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<DataRecord[]> Handle(TransformDataCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Transforming file {FileName} to {DataType} format", 
            request.SourceFile.FileName, request.DataType);

        return await _dataTransformer.TransformAsync(request.SourceFile, request.DataType, cancellationToken);
    }
}
