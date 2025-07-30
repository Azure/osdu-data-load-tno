using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for generating manifests with enhanced progress tracking
/// </summary>
public class GenerateManifestCommandHandler : IRequestHandler<GenerateManifestCommand, LoadingManifest>
{
    private readonly IManifestGenerator _manifestGenerator;
    private readonly ILogger<GenerateManifestCommandHandler> _logger;

    public GenerateManifestCommandHandler(IManifestGenerator manifestGenerator, ILogger<GenerateManifestCommandHandler> logger)
    {
        _manifestGenerator = manifestGenerator ?? throw new ArgumentNullException(nameof(manifestGenerator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadingManifest> Handle(GenerateManifestCommand request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("🚀 Generating manifest for {FileCount} {DataType} files", 
            request.SourceFiles.Length, request.DataType);

        // Note: Progress reporter should be set up via dependency injection
        // The ManifestGenerator will use its configured progress reporter if available

        var result = await _manifestGenerator.GenerateManifestAsync(request.SourceFiles, request.DataType, cancellationToken);
        
        _logger.LogInformation("✅ Manifest generation completed for {DataType}", request.DataType);
        
        return result;
    }
}
