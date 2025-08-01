using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for generating non-work product manifests from CSV data using IManifestGenerator
/// This generates reference data and master data manifests
/// </summary>
public class GenerateNonWorkProductManifestCommandHandler : IRequestHandler<GenerateNonWorkProductManifestCommand, LoadResult>
{
    private readonly ILogger<GenerateNonWorkProductManifestCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;
    private readonly IManifestGenerator _manifestGenerator;

    public GenerateNonWorkProductManifestCommandHandler(
        ILogger<GenerateNonWorkProductManifestCommandHandler> logger,
        IOptions<OsduConfiguration> configuration,
        IManifestGenerator manifestGenerator)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
        _manifestGenerator = manifestGenerator ?? throw new ArgumentNullException(nameof(manifestGenerator));
    }

    public async Task<LoadResult> Handle(GenerateNonWorkProductManifestCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting non-work product manifest generation from {SourcePath}", request.SourceDataPath);

        if (string.IsNullOrWhiteSpace(request.SourceDataPath))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Source data path is required",
                Duration = DateTime.UtcNow - startTime
            };
        }

        if (!Directory.Exists(request.SourceDataPath))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Source directory does not exist: {request.SourceDataPath}",
                Duration = DateTime.UtcNow - startTime
            };
        }

        try
        {
           
            // Get manifest configurations from the command
            var manifestGenerations = request.ManifestConfigs.ToArray();
            
            if (!manifestGenerations.Any())
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = "No manifest configurations provided",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            var totalGenerations = manifestGenerations.Length;
            var completedGenerations = 0;

            foreach (var generation in manifestGenerations)
            {
                _logger.LogInformation("Generating {Type} manifests using {MappingFile}", generation.Type, generation.MappingFile);
                var outputPath = Path.Combine(request.OutputPath, generation.OutputDir);
                var success = await GenerateManifestGroup(
                    request.SourceDataPath, 
                    generation.MappingFile, 
                    generation.Type, 
                    generation.DataDir, 
                    outputPath, 
                    generation.GroupFile, 
                    request.DataPartition,
                    request.AclViewer,
                    request.AclOwner,
                    request.LegalTag,
                    cancellationToken);

                if (!success)
                {
                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = $"Failed to generate {generation.Type} manifests",
                        Duration = DateTime.UtcNow - startTime
                    };
                }

                completedGenerations++;
                _logger.LogInformation("Completed {Type} manifests ({Completed}/{Total})", 
                    generation.Type, completedGenerations, totalGenerations);
            }

            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformation("Non-work product manifest generation completed in {Duration:mm\\:ss}", duration);

            return new LoadResult
            {
                IsSuccess = true,
                Message = $"Successfully generated non-work product manifests in '{request.OutputPath}'",
                ProcessedRecords = totalGenerations,
                SuccessfulRecords = completedGenerations,
                Duration = duration
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during non-work product manifest generation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to generate non-work product manifests",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }

    private async Task<bool> GenerateManifestGroup(
        string sourceDataPath, 
        string mappingFile, 
        string templateType, 
        string dataDir, 
        string outputDir, 
        bool groupFile, 
        string dataPartition,
        string aclViewer,
        string aclOwner,
        string legalTag,
        CancellationToken cancellationToken)
    {
        try
        {
            // Create output directory
            Directory.CreateDirectory(outputDir);

            var configDir = Path.Combine(sourceDataPath, "config");
            var mappingFilePath = Path.Combine(configDir, mappingFile);

            if (!File.Exists(mappingFilePath))
            {
                _logger.LogError("Mapping file not found: {MappingFile}", mappingFilePath);
                return false;
            }

            // Use the manifest generator to call Python script
            var success = await _manifestGenerator.GenerateManifestsFromCsvAsync(
                mappingFilePath,
                templateType,
                dataDir,
                outputDir,
                sourceDataPath,
                dataPartition,
                aclViewer,
                aclOwner,
                legalTag,
                groupFile,
                cancellationToken);

            if (!success)
            {
                _logger.LogError("Failed to generate manifests using manifest generator");
                return false;
            }

            _logger.LogInformation("Successfully generated manifests in: {OutputDir}", outputDir);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error generating manifest group");
            return false;
        }
    }
}
