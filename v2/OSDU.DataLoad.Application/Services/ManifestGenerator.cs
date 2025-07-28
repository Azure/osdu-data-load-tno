using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace OSDU.DataLoad.Application.Services;

/// <summary>
/// Service for generating loading manifests
/// </summary>
public class ManifestGenerator : IManifestGenerator
{
    private readonly ILogger<ManifestGenerator> _logger;
    private readonly JsonSerializerOptions _jsonOptions;

    public ManifestGenerator(ILogger<ManifestGenerator> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
    }

    public async Task<LoadingManifest> GenerateManifestAsync(SourceFile[] sourceFiles, TnoDataType dataType, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Generating manifest for {FileCount} files of type {DataType}", sourceFiles.Length, dataType);

        await Task.CompletedTask; // Placeholder for async operations

        var manifest = new LoadingManifest
        {
            Version = "1.0",
            Kind = dataType.ToString(),
            Description = $"Loading manifest for {dataType} data from TNO",
            SourceFiles = sourceFiles,
            Configuration = GetDataTypeConfiguration(dataType),
            Dependencies = GetDataTypeDependencies(dataType),
            CreatedAt = DateTime.UtcNow
        };

        _logger.LogInformation("Generated manifest with {FileCount} source files", manifest.SourceFiles.Length);

        return manifest;
    }

    public async Task<ValidationResult> ValidateManifestAsync(LoadingManifest manifest, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Validating manifest for {Kind}", manifest.Kind);

        var errors = new List<string>();
        var warnings = new List<string>();

        await Task.CompletedTask; // Placeholder for async operations

        // Validate basic structure
        if (string.IsNullOrEmpty(manifest.Version))
            errors.Add("Manifest version is required");

        if (string.IsNullOrEmpty(manifest.Kind))
            errors.Add("Manifest kind is required");

        if (!Enum.TryParse<TnoDataType>(manifest.Kind, out _))
            errors.Add($"Unknown data type: {manifest.Kind}");

        if (manifest.SourceFiles.Length == 0)
            errors.Add("No source files specified in manifest");

        // Validate source files
        foreach (var sourceFile in manifest.SourceFiles)
        {
            if (string.IsNullOrEmpty(sourceFile.FilePath))
                errors.Add($"Source file path is required");

            if (string.IsNullOrEmpty(sourceFile.FileName))
                errors.Add($"Source file name is required");

            if (!File.Exists(sourceFile.FilePath))
                warnings.Add($"Source file not found: {sourceFile.FilePath}");
        }

        // Check for duplicate files
        var duplicateFiles = manifest.SourceFiles
            .GroupBy(f => f.FilePath)
            .Where(g => g.Count() > 1)
            .Select(g => g.Key);

        foreach (var duplicate in duplicateFiles)
        {
            warnings.Add($"Duplicate source file: {duplicate}");
        }

        var isValid = errors.Count == 0;

        _logger.LogInformation("Manifest validation completed. Valid: {IsValid}, Errors: {ErrorCount}, Warnings: {WarningCount}",
            isValid, errors.Count, warnings.Count);

        return new ValidationResult
        {
            IsValid = isValid,
            Errors = errors.ToArray(),
            Warnings = warnings.ToArray()
        };
    }

    public async Task SaveManifestAsync(LoadingManifest manifest, string filePath, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Saving manifest to {FilePath}", filePath);

        try
        {
            var jsonContent = JsonSerializer.Serialize(manifest, _jsonOptions);
            await File.WriteAllTextAsync(filePath, jsonContent, cancellationToken);

            _logger.LogInformation("Manifest saved successfully to {FilePath}", filePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save manifest to {FilePath}", filePath);
            throw;
        }
    }

    private Dictionary<string, object> GetDataTypeConfiguration(TnoDataType dataType)
    {
        return dataType switch
        {
            TnoDataType.Wells => new Dictionary<string, object>
            {
                { "batchSize", 100 },
                { "validationLevel", "strict" },
                { "requireWellborePath", true },
                { "defaultLegalTags", new[] { "TNO-Well-Public" } }
            },
            TnoDataType.Wellbores => new Dictionary<string, object>
            {
                { "batchSize", 200 },
                { "validationLevel", "strict" },
                { "requireParentWell", true },
                { "defaultLegalTags", new[] { "TNO-Wellbore-Public" } }
            },
            TnoDataType.WellboreTrajectories => new Dictionary<string, object>
            {
                { "batchSize", 50 },
                { "validationLevel", "normal" },
                { "requireWellbore", true },
                { "defaultLegalTags", new[] { "TNO-Trajectory-Public" } }
            },
            TnoDataType.WellLogs => new Dictionary<string, object>
            {
                { "batchSize", 25 },
                { "validationLevel", "normal" },
                { "requireWellbore", true },
                { "defaultLegalTags", new[] { "TNO-WellLog-Public" } }
            },
            TnoDataType.ReferenceData => new Dictionary<string, object>
            {
                { "batchSize", 500 },
                { "validationLevel", "normal" },
                { "defaultLegalTags", new[] { "TNO-Reference-Public" } }
            },
            _ => new Dictionary<string, object>
            {
                { "batchSize", 100 },
                { "validationLevel", "normal" },
                { "defaultLegalTags", new[] { "TNO-Data-Public" } }
            }
        };
    }

    private string[] GetDataTypeDependencies(TnoDataType dataType)
    {
        return dataType switch
        {
            TnoDataType.Wellbores => new[] { "Wells" },
            TnoDataType.WellboreTrajectories => new[] { "Wells", "Wellbores" },
            TnoDataType.WellMarkers => new[] { "Wells" },
            TnoDataType.WellboreMarkers => new[] { "Wells", "Wellbores" },
            TnoDataType.WellLogs => new[] { "Wells", "Wellbores" },
            TnoDataType.WellCompletions => new[] { "Wells", "Wellbores" },
            _ => Array.Empty<string>()
        };
    }
}
