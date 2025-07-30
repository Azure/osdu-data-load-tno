using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for generating all TNO manifests from CSV data using templates
/// This corresponds to the "GenerateManifests" step in the Python solution
/// </summary>
public class GenerateManifestsCommandHandler : IRequestHandler<GenerateManifestsCommand, LoadResult>
{
    private readonly ILogger<GenerateManifestsCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;
    private readonly IManifestGenerator _manifestGenerator;

    public GenerateManifestsCommandHandler(
        ILogger<GenerateManifestsCommandHandler> logger,
        IOptions<OsduConfiguration> configuration,
        IManifestGenerator manifestGenerator)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
        _manifestGenerator = manifestGenerator ?? throw new ArgumentNullException(nameof(manifestGenerator));
    }

    public async Task<LoadResult> Handle(GenerateManifestsCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting manifest generation from {SourcePath}", request.SourceDataPath);

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

        var manifestDir = Path.Combine(request.OutputPath, "manifests");
        var dataPartition = string.IsNullOrWhiteSpace(request.DataPartition) 
            ? _configuration.DataPartition 
            : request.DataPartition;

        try
        {
            // Remove existing manifests directory
            if (Directory.Exists(manifestDir))
            {
                Directory.Delete(manifestDir, true);
            }

            // Create manifests directory structure
            Directory.CreateDirectory(manifestDir);

            var manifestGenerations = new[]
            {
                new { 
                    Type = "reference_data", 
                    MappingFile = "tno_ref_data_template_mapping.json",
                    DataDir = "reference-data",
                    OutputDir = "reference-manifests",
                    GroupFile = true
                },
                new { 
                    Type = "master_data", 
                    MappingFile = "tno_misc_master_data_template_mapping.json",
                    DataDir = "master-data/Misc_master_data",
                    OutputDir = "misc-master-data-manifests",
                    GroupFile = true
                },
                new { 
                    Type = "master_data", 
                    MappingFile = "tno_well_data_template_mapping.json",
                    DataDir = "master-data/Well",
                    OutputDir = "master-well-data-manifests",
                    GroupFile = false
                },
                new { 
                    Type = "master_data", 
                    MappingFile = "tno_wellbore_data_template_mapping.json",
                    DataDir = "master-data/Wellbore",
                    OutputDir = "master-wellbore-data-manifests",
                    GroupFile = false
                }
            };

            var totalGenerations = manifestGenerations.Length;
            var completedGenerations = 0;

            foreach (var generation in manifestGenerations)
            {
                _logger.LogInformation("Generating {Type} manifests using {MappingFile}", generation.Type, generation.MappingFile);
                
                var success = await GenerateManifestGroup(
                    request.SourceDataPath, 
                    generation.MappingFile, 
                    generation.Type, 
                    generation.DataDir, 
                    Path.Combine(manifestDir, generation.OutputDir), 
                    generation.GroupFile, 
                    dataPartition,
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
            _logger.LogInformation("Manifest generation completed in {Duration:mm\\:ss}", duration);

            return new LoadResult
            {
                IsSuccess = true,
                Message = $"Successfully generated all manifests in '{manifestDir}'",
                ProcessedRecords = totalGenerations,
                SuccessfulRecords = completedGenerations,
                Duration = duration
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during manifest generation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to generate manifests",
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

            // Use the C# manifest generator instead of Python scripts
            var success = await _manifestGenerator.GenerateManifestsFromCsvAsync(
                mappingFilePath,
                templateType,
                dataDir,
                outputDir,
                sourceDataPath,
                dataPartition,
                groupFile,
                cancellationToken);

            if (!success)
            {
                _logger.LogError("Failed to generate manifests using C# generator");
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
