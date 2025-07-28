using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Queries;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for validating manifests
/// </summary>
public class ValidateManifestQueryHandler : IRequestHandler<ValidateManifestQuery, ValidationResult>
{
    private readonly IManifestGenerator _manifestGenerator;
    private readonly ILogger<ValidateManifestQueryHandler> _logger;

    public ValidateManifestQueryHandler(IManifestGenerator manifestGenerator, ILogger<ValidateManifestQueryHandler> logger)
    {
        _manifestGenerator = manifestGenerator ?? throw new ArgumentNullException(nameof(manifestGenerator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<ValidationResult> Handle(ValidateManifestQuery request, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Validating manifest with {FileCount} files", 
            request.Manifest.SourceFiles.Length);

        return await _manifestGenerator.ValidateManifestAsync(request.Manifest, cancellationToken);
    }
}
