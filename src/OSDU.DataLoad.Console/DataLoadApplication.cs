using MediatR;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Console;

/// <summary>
/// Main application class for handling command line operations
/// </summary>
public class DataLoadApplication
{
    private readonly IMediator _mediator;
    private readonly ILogger<DataLoadApplication> _logger;
    private readonly IConfiguration _configuration;

    public DataLoadApplication(IMediator mediator, ILogger<DataLoadApplication> logger, IConfiguration configuration)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }

    /// <summary>
    /// Main entry point for handling command line arguments
    /// </summary>
    public async Task<int> RunAsync(string[] args)
    {
        _logger.LogInformation("OSDU Data Load TNO v1.0 - Starting application");

        try
        {
            int exitCode;
            if (args.Length == 0)
            {
            // Default behavior: download data if needed, then load it
            exitCode = await HandleDefaultCommand();
            }
            else
            {
            var command = args[0].ToLowerInvariant();

            exitCode = command switch
            {
                "load" => await HandleLoadCommand(args),
                "download" => await HandleDownloadCommand(args),
                "help" or "--help" or "-h" => ShowHelp(),
                _ => ShowHelp("Unknown command: " + command)
            };
            }

            // Sleep forever if completed successfully
            await Task.Delay(Timeout.Infinite);
            return exitCode;
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Unexpected application error occurred");
            return 99; // General application error
        }
    }

    private async Task<int> HandleLoadCommand(string[] args)
    {
        if (args.Length < 3)
        {
            _logger.LogError("Invalid usage for load command: insufficient arguments");
            return -1;
        }

        string? source = null;

        for (int i = 1; i < args.Length; i++)
        {
            switch (args[i].ToLowerInvariant())
            {
                case "--source" when i + 1 < args.Length:
                    source = args[++i];
                    break;
            }
        }

        if (string.IsNullOrEmpty(source))
        {
            _logger.LogError("Load command requires --source parameter");
            return -1;
        }

        _logger.LogInformation("Starting OSDU Data Load TNO - Load Command");
        _logger.LogInformation("Source directory: {Source}", source);
        
        DisplayConfigurationStatus();

        await LoadAllDataAsync(source);
        return 0;
    }

    private async Task<int> HandleDownloadCommand(string[] args)
    {
        if (args.Length < 3)
        {
            _logger.LogError("Invalid usage for download command: insufficient arguments");
            return -1;
        }

        string? destination = null;
        bool overwrite = false;

        for (int i = 1; i < args.Length; i++)
        {
            switch (args[i].ToLowerInvariant())
            {
                case "--destination" when i + 1 < args.Length:
                    destination = args[++i];
                    break;
                case "--overwrite":
                    overwrite = true;
                    break;
            }
        }

        if (string.IsNullOrEmpty(destination))
        {
            _logger.LogError("Download command requires --destination parameter");
            return -1;
        }

        _logger.LogInformation("Starting OSDU Data Load TNO - Download Command");
        _logger.LogInformation("Destination directory: {Destination}", destination);
        _logger.LogInformation("Overwrite existing: {Overwrite}", overwrite);
        
        DisplayConfigurationStatus();

        await DownloadDataAsync(destination, overwrite);
        return 0;
    }

    private async Task<int> HandleDefaultCommand()
    {
        // Use cross-platform default path
        var defaultDataPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "osdu-data", "tno");
        
        _logger.LogInformation("No command specified - running default behavior");
        _logger.LogInformation("Default data path: {DefaultDataPath}", defaultDataPath);

        _logger.LogInformation("Starting OSDU Data Load TNO - Default Mode");
        _logger.LogInformation("Using default data directory: {DefaultDataPath}", defaultDataPath);
        
        DisplayConfigurationStatus();

        // Check if data already exists
        bool dataExists = CheckIfDataExists(defaultDataPath);
        
        if (!dataExists)
        {
            _logger.LogInformation("TNO test data not found - downloading automatically");

            try
            {
                var downloadResult = await _mediator.Send(new DownloadDataCommand
                {
                    DestinationPath = defaultDataPath,
                    OverwriteExisting = false
                });

                DisplayDownloadResult(downloadResult);

                if (!downloadResult.IsSuccess)
                {
                    _logger.LogError("Failed to download TNO data. Cannot proceed with load operation");
                    return -1;
                }

                _logger.LogInformation("Download completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during automatic TNO data download");
                return -1;
            }
        }
        else
        {
            _logger.LogInformation("TNO test data found - proceeding with load operation");
        }

        // Now run the load operation
        _logger.LogInformation("Starting data load operation");

        try
        {
            await LoadAllDataAsync(defaultDataPath);
            return 0;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during default load operation");
            return -1;
        }
    }

    private bool CheckIfDataExists(string dataPath)
    {
        if (!Directory.Exists(dataPath))
        {
            _logger.LogInformation("Data directory does not exist: {DataPath}", dataPath);
            return false;
        }

        // Check for key directories that indicate TNO data is present
        var requiredDirectories = new[]
        {
            Path.Combine(dataPath, "manifests"),
            Path.Combine(dataPath, "datasets"),
            Path.Combine(dataPath, "TNO")
        };

        foreach (var dir in requiredDirectories)
        {
            if (!Directory.Exists(dir))
            {
                _logger.LogInformation("Required directory missing: {Directory}", dir);
                return false;
            }
        }

        // Check if there are any manifest files
        var manifestsPath = Path.Combine(dataPath, "manifests");
        var hasManifests = Directory.GetDirectories(manifestsPath).Any(d => 
            Directory.GetFiles(d, "*.json").Length > 0);

        if (!hasManifests)
        {
            _logger.LogInformation("No manifest files found in: {ManifestsPath}", manifestsPath);
            return false;
        }

        _logger.LogInformation("TNO data appears to be present at: {DataPath}", dataPath);
        return true;
    }

    private int ShowHelp(string? error = null)
    {
        if (!string.IsNullOrEmpty(error))
        {
            _logger.LogError("Help requested due to error: {Error}", error);
        }

        _logger.LogInformation("OSDU Data Load TNO v1.0 - Loads TNO data into OSDU platform");
        _logger.LogInformation("Default Behavior (no arguments): When run without arguments, the application will:");
        _logger.LogInformation("  1. Check for TNO data in {DefaultDataPath}", GetDefaultDataPath());
        _logger.LogInformation("  2. Download the data if not present");
        _logger.LogInformation("  3. Load all data types into OSDU platform");
        _logger.LogInformation("Commands:");
        _logger.LogInformation("  load         Load all TNO data types into OSDU platform in the correct order");
        _logger.LogInformation("  download     Download and setup TNO test data from official repository");
        _logger.LogInformation("  help         Show this help message");
        _logger.LogInformation("Usage:");
        _logger.LogInformation("  load --source <path>");
        _logger.LogInformation("  download --destination <path> [--overwrite]");
        _logger.LogInformation("Description:");
        _logger.LogInformation("  Loads TNO data from the specified directory using the following steps:");
        _logger.LogInformation("    1. Create Legal Tag     (if configured)");
        _logger.LogInformation("    2. Upload Dataset Files (including mapping files required for work products)");
        _logger.LogInformation("    3. Generate Manifests  (non-work product first, then work product)");
        _logger.LogInformation("    4. Submit Workflow      (to OSDU platform)");
        _logger.LogInformation("Download TNO Test Data:");
        _logger.LogInformation("  download-tno --destination <path>    Download and setup test data");
        _logger.LogInformation("  download-tno --destination <path> --overwrite    Overwrite existing data");
        _logger.LogInformation("Examples:");
        _logger.LogInformation("  dotnet run                           # Default: download data to {DefaultDataPath} and load", GetDefaultDataPath());
        _logger.LogInformation("  download-tno --destination \"C:\\Data\\open-test-data\"");
        _logger.LogInformation("  load --source \"C:\\Data\\open-test-data\"");
        
        DisplayConfigurationStatus();

        return string.IsNullOrEmpty(error) ? 0 : -1;
    }

    private string GetConfigValue(string key, string defaultValue = "")
    {
        // First try environment variable
        var envValue = _configuration[key];
        if (!string.IsNullOrEmpty(envValue))
            return envValue;
            
        // Then try appsettings.json
        var settingsValue = _configuration[$"Osdu:{key}"];
        if (!string.IsNullOrEmpty(settingsValue))
            return settingsValue;
            
        return defaultValue;
    }

    private static bool IsConfigured(string value)
    {
        return !string.IsNullOrEmpty(value) && 
               !value.StartsWith("your-", StringComparison.OrdinalIgnoreCase) &&
               value != "string.Empty";
    }

    /// <summary>
    /// Validates that all required configuration values are present for the load operation
    /// </summary>
    /// <returns>True if all required values are configured, false otherwise</returns>
    private bool ValidateRequiredConfiguration()
    {
        var requiredSettings = new[]
        {
            ("BaseUrl", GetConfigValue("BaseUrl")),
            ("TenantId", GetConfigValue("TenantId")),
            ("ClientId", GetConfigValue("ClientId")),
            ("LegalTag", GetConfigValue("LegalTag")),
            ("AclOwner", GetConfigValue("AclOwner"))
        };

        var missingSettings = requiredSettings
            .Where(setting => !IsConfigured(setting.Item2))
            .Select(setting => setting.Item1)
            .ToList();

        if (missingSettings.Any())
        {
            _logger.LogError("Missing required configuration values:");
            foreach (var setting in missingSettings)
            {
                _logger.LogError("- OSDU_{Setting} is not configured", setting);
            }
            _logger.LogError("These configuration values are required for the application to run.");
            _logger.LogError("Please set them as environment variables (e.g., OSDU_BaseUrl) or in appsettings.json.");
            return false;
        }

        _logger.LogInformation("All required configuration values are present");
        return true;
    }

    private static string GetDefaultDataPath()
    {
        return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "osdu-data", "tno");
    }

    private void DisplayConfigurationStatus()
    {
        _logger.LogInformation("Environment Variables Configuration:");
        _logger.LogInformation("Configure OSDU settings using environment variables with OSDU_ prefix:");
        _logger.LogInformation("OSDU_BaseUrl       = {BaseUrl}", GetConfigValue("BaseUrl"));
        _logger.LogInformation("OSDU_TenantId      = {TenantId}", GetConfigValue("TenantId"));
        _logger.LogInformation("OSDU_ClientId      = {ClientId}", GetConfigValue("ClientId"));
        _logger.LogInformation("OSDU_DataPartition = {DataPartition}", GetConfigValue("DataPartition"));
        _logger.LogInformation("OSDU_LegalTag      = {LegalTag}", GetConfigValue("LegalTag"));
        _logger.LogInformation("OSDU_AclViewer     = {AclViewer}", GetConfigValue("AclViewer"));
        _logger.LogInformation("OSDU_AclOwner      = {AclOwner}", GetConfigValue("AclOwner"));
        _logger.LogInformation("OSDU_UserEmail     = {UserEmail}", GetConfigValue("UserEmail"));

        _logger.LogInformation("Current Configuration Status:");
        var baseUrl = GetConfigValue("BaseUrl");
        var tenantId = GetConfigValue("TenantId");
        var clientId = GetConfigValue("ClientId");
        var dataPartition = GetConfigValue("DataPartition");
        var legalTag = GetConfigValue("LegalTag");
        var aclViewer = GetConfigValue("AclViewer");
        var aclOwner = GetConfigValue("AclOwner");
        var userEmail = GetConfigValue("UserEmail");

        _logger.LogInformation("BaseUrl: {Status} (REQUIRED)", IsConfigured(baseUrl) ? "Configured" : "Not configured");
        _logger.LogInformation("TenantId: {Status} (REQUIRED)", IsConfigured(tenantId) ? "Configured" : "Not configured");
        _logger.LogInformation("ClientId: {Status} (REQUIRED)", IsConfigured(clientId) ? "Configured" : "Not configured");
        _logger.LogInformation("DataPartition: {Status}", IsConfigured(dataPartition) ? "Configured" : "Not configured");
        _logger.LogInformation("LegalTag: {Status} (REQUIRED)", IsConfigured(legalTag) ? "Configured" : "Not configured");
        _logger.LogInformation("AclViewer: {Status}", IsConfigured(aclViewer) ? "Configured" : "Not configured");
        _logger.LogInformation("AclOwner: {Status} (REQUIRED)", IsConfigured(aclOwner) ? "Configured" : "Not configured");
        _logger.LogInformation("UserEmail: {Status}", IsConfigured(userEmail) ? "Configured (user will be added to ops group)" : "Not configured (user authorization setup will be skipped)");
    }

    private async Task LoadAllDataAsync(string source)
    {
        _logger.LogInformation("Starting complete TNO data load operation");
        _logger.LogInformation("Source: {Source}", source);

        // Validate required configuration before proceeding
        if (!ValidateRequiredConfiguration())
        {
            _logger.LogError("Cannot proceed with load operation due to missing required configuration");
            return;
        }

        try
        {
            _logger.LogInformation("Loading all TNO data types from {Source}", source);

            // Get OSDU configuration for command parameters
            var dataPartition = GetConfigValue("DataPartition");
            var legalTag = GetConfigValue("LegalTag");
            var aclViewer = GetConfigValue("AclViewer");
            var aclOwner = GetConfigValue("AclOwner");

            var overallSuccess = true;
            var startTime = DateTime.UtcNow;

            // Step 1: Create Legal Tag
            _logger.LogInformation("Step 1: Creating legal tag");
            var legalTagResult = await _mediator.Send(new CreateLegalTagCommand
            {
                LegalTagName = legalTag
            });
            DisplayStepResult("Legal Tag Creation", legalTagResult);
            overallSuccess = overallSuccess && legalTagResult.IsSuccess;

            //// Step 2: Upload Dataset Files 
            //if (overallSuccess)
            //{
                //_logger.LogInformation("Step 2: Uploading dataset files from configured directories");
                //var uploadResult = await _mediator.Send(new UploadFilesCommand(source, Path.Combine(source, "output")));
                //DisplayStepResult("Dataset File Upload", uploadResult);
                //var uploadSuccess = uploadResult.IsSuccess;
                //overallSuccess = overallSuccess && uploadResult.IsSuccess;
            //}
            //else
            //{
            //    _logger.LogError("Skipping data set upload due to legal tag creation failure");
            //}

            ////Step 3: Generate Manifests
            //if (overallSuccess)
            //{
                _logger.LogInformation("Step 3: Generating manifests (datasets uploaded, can now generate work products)");
                var manifestResult = await _mediator.Send(new GenerateManifestsCommand
                {
                    SourceDataPath = source,
                    OutputPath = source,
                    DataPartition = dataPartition,
                    LegalTag = legalTag,
                    AclViewer = aclViewer,
                    AclOwner = aclOwner
                });
                DisplayStepResult("Manifest Generation", manifestResult);
                overallSuccess = overallSuccess && manifestResult.IsSuccess;
            //}
            //else
            //{
            //    _logger.LogError("Skipping manifest generate due to data set upload failure");
            //}

            //// Step 4: Submit Manifests to Workflow Service
            //if (overallSuccess)
            //{
                _logger.LogInformation("Step 4: Submitting manifests to workflow service");
                
                // Use the manifests directory that contains both work product and non-work product manifests
                var manifestsDirectory = Path.Combine(source, "manifests");

                var workflowResult = await _mediator.Send(new SubmitManifestsToWorkflowServiceCommand
                {
                    SourceDataPath = source,
                    DataPartition = dataPartition
                });
                DisplayStepResult("Manifest Workflow Submission", workflowResult);
                overallSuccess = overallSuccess && workflowResult.IsSuccess;
             //}
             //else
             //{
             //    _logger.LogError("Skipping workflow submission due to manifest generation failure");
             //}
            
            // Display overall results
            var totalDuration = DateTime.UtcNow - startTime;
            _logger.LogInformation("Overall Result: {Result}, Total Duration: {Duration:mm\\:ss}", 
                overallSuccess ? "Success" : "Failed", totalDuration);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during load operation");
        }
    }

    private static bool IsUploadableFile(string filePath)
    {
        var extension = Path.GetExtension(filePath).ToLowerInvariant();
        var excludedExtensions = new[] { ".json", ".log", ".txt", ".md" };
        return !excludedExtensions.Contains(extension) && !Path.GetFileName(filePath).StartsWith(".");
    }

    private void DisplayStepResult(string stepName, LoadResult result)
    {
        _logger.LogInformation("{StepName}: {Status}", stepName, result.IsSuccess ? "Success" : "Failed");
        if (result.ProcessedRecords > 0)
        {
            _logger.LogInformation("Processed: {ProcessedRecords}, Successful: {SuccessfulRecords}, Failed: {FailedRecords}", 
                result.ProcessedRecords, result.SuccessfulRecords, result.FailedRecords);
        }
        if (!string.IsNullOrEmpty(result.Message))
        {
            _logger.LogInformation("Message: {Message}", result.Message);
        }
        if (!result.IsSuccess && !string.IsNullOrEmpty(result.ErrorDetails))
        {
            _logger.LogError("Error: {ErrorDetails}", result.ErrorDetails);
        }
    }

    private void DisplayLoadResult(LoadResult result)
    {
        _logger.LogInformation("Load Results - Status: {Status}", result.IsSuccess ? "Success" : "Failed");
        _logger.LogInformation("Processed Records: {ProcessedRecords}", result.ProcessedRecords);
        _logger.LogInformation("Successful Records: {SuccessfulRecords}", result.SuccessfulRecords);
        _logger.LogInformation("Failed Records: {FailedRecords}", result.FailedRecords);
        _logger.LogInformation("Duration: {Duration}", result.Duration);

        if (!string.IsNullOrEmpty(result.Message))
        {
            _logger.LogInformation("Message: {Message}", result.Message);
        }

        if (!string.IsNullOrEmpty(result.ErrorDetails))
        {
            _logger.LogError("Error Details: {ErrorDetails}", result.ErrorDetails);
        }

        if (result.ProcessedRecords > 0)
        {
            var successRate = (double)result.SuccessfulRecords / result.ProcessedRecords * 100;
            _logger.LogInformation("Success Rate: {SuccessRate:F1}%", successRate);
        }
    }

    private void DisplayDownloadResult(LoadResult result)
    {
        _logger.LogInformation("Download Results - Status: {Status}", result.IsSuccess ? "Success" : "Failed");
        _logger.LogInformation("Duration: {Duration}", result.Duration);

        if (!string.IsNullOrEmpty(result.Message))
        {
            _logger.LogInformation("Message: {Message}", result.Message);
        }

        if (!string.IsNullOrEmpty(result.ErrorDetails))
        {
            _logger.LogError("Error Details: {ErrorDetails}", result.ErrorDetails);
        }
    }

    private async Task DownloadDataAsync(string destination, bool overwrite)
    {
        _logger.LogInformation("Starting TNO test data download");
        _logger.LogInformation("Destination: {Destination}", destination);
        _logger.LogInformation("Overwrite: {Overwrite}", overwrite);

        try
        {
            _logger.LogInformation("Downloading TNO test data to {Destination}", destination);

            var result = await _mediator.Send(new DownloadDataCommand
            {
                DestinationPath = destination,
                OverwriteExisting = overwrite
            });

            DisplayDownloadResult(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TNO data download");
        }
    }
}
