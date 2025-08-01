using System.Diagnostics;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace OSDU.DataLoad.Infrastructure.Services
{
    public class ManifestGenerator : IManifestGenerator
    {
        private readonly ILogger<ManifestGenerator> _logger;

        public ManifestGenerator(ILogger<ManifestGenerator> logger)
        {
            _logger = logger;
        }

        private async Task<ManifestMappingConfig?> LoadMappingConfigAsync(string mappingFilePath, CancellationToken cancellationToken)
        {
            try
            {
                var jsonOptions = new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    WriteIndented = true,
                    PropertyNameCaseInsensitive = true
                };
                var json = await File.ReadAllTextAsync(mappingFilePath, cancellationToken);
                return JsonSerializer.Deserialize<ManifestMappingConfig>(json, jsonOptions);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load mapping configuration from: {MappingFile}", mappingFilePath);
                return null;
            }
        }

        public async Task<bool> GenerateManifestsFromCsvAsync(
            string mappingFilePath,
            string templateType,
            string dataDirectory,
            string outputDirectory,
            string homeDirectory,
            string dataPartition,
            string aclViewer,
            string aclOwner,
            string legalTag = null,
            bool groupFile = false,
            CancellationToken cancellationToken = default)
        {
            try
            {
                _logger.LogInformation("Enhanced manifest generation from mapping file: {MappingFile}", mappingFilePath);

                var mappingConfig = await LoadMappingConfigAsync(mappingFilePath, cancellationToken);
                if (mappingConfig == null)
                    return false;

                Directory.CreateDirectory(outputDirectory);

                var templatesDir = Path.Combine(homeDirectory, "templates", templateType);
                var csvDataDir = Path.Combine(homeDirectory, "TNO", "contrib", dataDirectory);

                foreach (var mapping in mappingConfig.Mapping)
                {
                    var csvFilePath = Path.Combine(csvDataDir, mapping.DataFile);
                    var templateFilePath = Path.Combine(templatesDir, mapping.TemplateFile);

                    if (!File.Exists(csvFilePath) || !File.Exists(templateFilePath))
                    {
                        _logger.LogWarning("Missing file - CSV: {CsvExists}, Template: {TemplateExists}",
                            File.Exists(csvFilePath), File.Exists(templateFilePath));
                        continue;
                    }

                    _logger.LogInformation("Processing mapping: {DataFile} -> {TemplateFile}", mapping.DataFile, mapping.TemplateFile);

                    try
                    {
                        // Call CreateManifestFromCsv with proper parameters
                        GenerateManifestWithPython(
                            inputCsv: csvFilePath,
                            templateJson: templateFilePath,
                            outputPath: outputDirectory,
                            dataPartition: dataPartition,
                            requiredTemplate: mappingConfig.RequiredTemplate != null ? JsonSerializer.Serialize(mappingConfig.RequiredTemplate) : null,
                            groupFilename: groupFile ? mapping.OutputFileName : null,
                            aclViewer: aclViewer,
                            aclOwner: aclOwner,
                            legalTag: legalTag
                        );

                        _logger.LogInformation("Successfully processed mapping for {DataFile}", mapping.DataFile);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to process mapping for {DataFile}", mapping.DataFile);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in enhanced manifest generation");
                return false;
            }
        }

        private void GenerateManifestWithPython(string inputCsv, string templateJson, string outputPath,
            string dataPartition = null,
            string requiredTemplate = null,
            string groupFilename = null,
            string aclViewer = null,
            string aclOwner = null,
            string legalTag = null)
        {
            try
            {
                _logger.LogInformation("Calling Python script to generate manifests from CSV: {InputCsv}", inputCsv);
                
                // Build arguments for Python script
                var args = new List<string>
                {
                    inputCsv,
                    templateJson,
                    outputPath
                };

                string tempRequiredTemplateFile = null;

                // Add optional parameters
                if (!string.IsNullOrEmpty(dataPartition))
                {
                    args.Add($"--schema-ns-value={dataPartition}");
                }

                if (!string.IsNullOrEmpty(requiredTemplate))
                {
                    // Write required template to a temporary file to avoid command line escaping issues
                    tempRequiredTemplateFile = Path.GetTempFileName();
                    File.WriteAllText(tempRequiredTemplateFile, requiredTemplate);
                    args.Add($"--required-template-file={tempRequiredTemplateFile}");
                }

                if (!string.IsNullOrEmpty(groupFilename))
                {
                    args.Add($"--group-filename={groupFilename}");
                }

                if (!string.IsNullOrEmpty(aclViewer))
                {
                    args.Add($"--acl-viewer={aclViewer}");
                }

                if (!string.IsNullOrEmpty(aclOwner))
                {
                    args.Add($"--acl-owner={aclOwner}");
                }

                if (!string.IsNullOrEmpty(legalTag))
                {
                    args.Add($"--legal-tag={legalTag}");
                }

                try
                {
                    // Call Python script
                    CallPythonScript(args.ToArray());
                    
                    _logger.LogInformation("Successfully generated manifests using Python script");
                }
                finally
                {
                    // Clean up temporary file
                    if (tempRequiredTemplateFile != null && File.Exists(tempRequiredTemplateFile))
                    {
                        try
                        {
                            File.Delete(tempRequiredTemplateFile);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete temporary file: {TempFile}", tempRequiredTemplateFile);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to generate manifests using Python script");
                throw;
            }
        }

        private void CallPythonScript(string[] args)
        {
            try
            {
                // Find the Python script path relative to the repository root
                // Current directory when running dotnet run is: c:\Users\kymille\src\energy\osdu-data-load-tno\src
                // We need to go up one level to reach the repository root: c:\Users\kymille\src\energy\osdu-data-load-tno
                var currentDir = Directory.GetCurrentDirectory();
                var repoRoot = Directory.GetParent(currentDir)?.FullName ?? "";
                var scriptPath = Path.Combine(repoRoot, "generate-manifest-scripts", "csv_to_json_wrapper.py");

                _logger.LogInformation("Current directory: {CurrentDir}", currentDir);
                _logger.LogInformation("Repository root: {RepoRoot}", repoRoot);
                _logger.LogInformation("Looking for script at: {ScriptPath}", scriptPath);

                if (!File.Exists(scriptPath))
                {
                    throw new FileNotFoundException($"Python script not found at: {scriptPath}");
                }

                _logger.LogInformation("Using Python script: {ScriptPath}", scriptPath);
                var scriptArgs = string.Join(" ", args.Select(arg => $"\"{arg}\""));
                var processInfo = new ProcessStartInfo
                {
                    FileName = "python",
                    Arguments = $"\"{scriptPath}\" {scriptArgs}",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true,
                    WorkingDirectory = Path.GetDirectoryName(scriptPath) // Set working directory to script location
                };

                _logger.LogInformation("Executing: {FileName} {Arguments}", processInfo.FileName, processInfo.Arguments);

                using var process = new Process();
                process.StartInfo = processInfo;

                // Set up event handlers for real-time output streaming
                process.OutputDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        _logger.LogInformation("[Python Output] {Data}", e.Data);
                    }
                };

                process.Start();

                // Begin asynchronous read operations
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

                // Wait for the process to complete
                process.WaitForExit();

                _logger.LogInformation("Python script finished with exit code: {ExitCode}", process.ExitCode);

                if (process.ExitCode != 0)
                {
                    _logger.LogError("Python script failed with exit code {ExitCode}", process.ExitCode);
                    throw new InvalidOperationException($"Python script failed with exit code: {process.ExitCode}");
                }

                _logger.LogInformation("Python script completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calling Python script");
                throw;
            }
        }
    }
}