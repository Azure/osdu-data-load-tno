using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for saving manifests
/// </summary>
public class SaveManifestCommandHandler : IRequestHandler<SaveManifestCommand>
{
    private readonly IManifestGenerator _manifestGenerator;
    private readonly ILogger<SaveManifestCommandHandler> _logger;

    public SaveManifestCommandHandler(IManifestGenerator manifestGenerator, ILogger<SaveManifestCommandHandler> logger)
    {
        _manifestGenerator = manifestGenerator ?? throw new ArgumentNullException(nameof(manifestGenerator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task Handle(SaveManifestCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Saving manifest to {FilePath}", request.FilePath);

        await _manifestGenerator.SaveManifestAsync(request.Manifest, request.FilePath, cancellationToken);
    }
}
