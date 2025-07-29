using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Queries;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for validating source data
/// </summary>
public class ValidateSourceQueryHandler : IRequestHandler<ValidateSourceQuery, ValidationResult>
{
    private readonly IDataTransformer _dataTransformer;
    private readonly ILogger<ValidateSourceQueryHandler> _logger;

    public ValidateSourceQueryHandler(IDataTransformer dataTransformer, ILogger<ValidateSourceQueryHandler> logger)
    {
        _dataTransformer = dataTransformer ?? throw new ArgumentNullException(nameof(dataTransformer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<ValidationResult> Handle(ValidateSourceQuery request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Validating source file {FileName}", request.SourceFile.FileName);

        return await _dataTransformer.ValidateSourceAsync(request.SourceFile, cancellationToken);
    }
}
