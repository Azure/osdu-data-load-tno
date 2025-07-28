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
            if (args.Length == 0)
            {
                ShowHelp();
                return 0;
            }

            var command = args[0].ToLowerInvariant();

            return command switch
            {
                "load" => await HandleLoadCommand(args),
                "download-tno" => await HandleDownloadTnoCommand(args),
                "help" or "--help" or "-h" => ShowHelp(),
                _ => ShowHelp("Unknown command: " + command)
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Application error");
            System.Console.WriteLine($"❌ Error: {ex.Message}");
            return -1;
        }
    }

    private async Task<int> HandleLoadCommand(string[] args)
    {
        if (args.Length < 3)
        {
            System.Console.WriteLine("❌ Usage: load --source <path>");
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
            System.Console.WriteLine("❌ --source is required");
            return -1;
        }

        await LoadAllDataAsync(source);
        return 0;
    }

    private async Task<int> HandleDownloadTnoCommand(string[] args)
    {
        if (args.Length < 3)
        {
            System.Console.WriteLine("❌ Usage: download-tno --destination <path> [--overwrite]");
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
            System.Console.WriteLine("❌ --destination is required");
            return -1;
        }

        await DownloadTnoDataAsync(destination, overwrite);
        return 0;
    }

    private int ShowHelp(string? error = null)
    {
        if (!string.IsNullOrEmpty(error))
        {
            System.Console.WriteLine($"❌ {error}");
            System.Console.WriteLine();
        }

        System.Console.WriteLine("OSDU Data Load TNO v1.0 - Loads TNO data into OSDU platform");
        System.Console.WriteLine();
        System.Console.WriteLine("Commands:");
        System.Console.WriteLine("  load         Load all TNO data types into OSDU platform in the correct order");
        System.Console.WriteLine("  download-tno Download and setup TNO test data from official repository");
        System.Console.WriteLine("  help         Show this help message");
        System.Console.WriteLine();
        System.Console.WriteLine("Usage:");
        System.Console.WriteLine("  load --source <path>");
        System.Console.WriteLine("  download-tno --destination <path> [--overwrite]");
        System.Console.WriteLine();
        System.Console.WriteLine("Description:");
        System.Console.WriteLine("  Loads TNO data from the specified directory in the following order:");
        System.Console.WriteLine("    1. Reference Data       (manifests/reference-manifests/)");
        System.Console.WriteLine("    2. Misc Master Data     (manifests/misc-master-data-manifests/)");
        System.Console.WriteLine("    3. Wells                (manifests/master-well-data-manifests/)");
        System.Console.WriteLine("    4. Wellbores            (manifests/master-wellbore-data-manifests/)");
        System.Console.WriteLine("    5. Documents            (datasets/documents/)");
        System.Console.WriteLine("    6. Well Logs            (datasets/well-logs/)");
        System.Console.WriteLine("    7. Well Markers         (datasets/markers/)");
        System.Console.WriteLine("    8. Wellbore Trajectories (datasets/trajectories/)");
        System.Console.WriteLine("    9. Work Products        (TNO/provided/TNO/work-products/)");
        System.Console.WriteLine();
        System.Console.WriteLine("Download TNO Test Data:");
        System.Console.WriteLine("  download-tno --destination <path>    Download and setup test data");
        System.Console.WriteLine("  download-tno --destination <path> --overwrite    Overwrite existing data");
        System.Console.WriteLine();
        System.Console.WriteLine("Examples:");
        System.Console.WriteLine("  download-tno --destination \"C:\\Data\\open-test-data\"");
        System.Console.WriteLine("  load --source \"C:\\Data\\open-test-data\"");
        System.Console.WriteLine();
        System.Console.WriteLine("Environment Variables:");
        System.Console.WriteLine("  Configure OSDU settings using environment variables with OSDU_ prefix:");
        System.Console.WriteLine($"  OSDU_BaseUrl       = {GetConfigValue("BaseUrl")}");
        System.Console.WriteLine($"  OSDU_TenantId      = {GetConfigValue("TenantId")}");
        System.Console.WriteLine($"  OSDU_ClientId      = {GetConfigValue("ClientId")}");
        System.Console.WriteLine($"  OSDU_DataPartition = {GetConfigValue("DataPartition")}");
        System.Console.WriteLine($"  OSDU_LegalTag      = {GetConfigValue("LegalTag")}");
        System.Console.WriteLine($"  OSDU_AclViewer     = {GetConfigValue("AclViewer")}");
        System.Console.WriteLine($"  OSDU_AclOwner      = {GetConfigValue("AclOwner")}");
        System.Console.WriteLine();
        System.Console.WriteLine("Current Configuration Status:");
        var baseUrl = GetConfigValue("BaseUrl");
        var tenantId = GetConfigValue("TenantId");
        var clientId = GetConfigValue("ClientId");
        var dataPartition = GetConfigValue("DataPartition");
        var legalTag = GetConfigValue("LegalTag");
        var aclViewer = GetConfigValue("AclViewer");
        var aclOwner= GetConfigValue("AclOwner");

        System.Console.WriteLine($"  ✓ BaseUrl: {(IsConfigured(baseUrl) ? "✅ Configured" : "❌ Not configured")}");
        System.Console.WriteLine($"  ✓ TenantId: {(IsConfigured(tenantId) ? "✅ Configured" : "❌ Not configured")}");
        System.Console.WriteLine($"  ✓ ClientId: {(IsConfigured(clientId) ? "✅ Configured" : "❌ Not configured")}");
        System.Console.WriteLine($"  ✓ DataPartition: {(IsConfigured(dataPartition) ? "✅ Configured" : "❌ Not configured")}");
        System.Console.WriteLine($"  ✓ LegalTag: {(IsConfigured(legalTag) ? "✅ Configured" : "❌ Not configured")}");
        System.Console.WriteLine($"  ✓ AclViewer: {(IsConfigured(aclViewer) ? "✅ Configured" : "❌ Not configured")}");
        System.Console.WriteLine($"  ✓ ActOwner: {(IsConfigured(aclOwner) ? "✅ Configured" : "❌ Not configured")}");

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

    private async Task LoadAllDataAsync(string source)
    {
        _logger.LogInformation("Starting complete TNO data load operation");
        _logger.LogInformation("Source: {Source}", source);

        try
        {
            System.Console.WriteLine($"🚀 Loading all TNO data types from {source}...");
            System.Console.WriteLine();

            var result = await _mediator.Send(new LoadAllDataCommand 
            { 
                SourcePath = source
            });

            DisplayLoadResult(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during load operation");
            System.Console.WriteLine($"❌ Load operation failed: {ex.Message}");
        }
    }

    private void DisplayLoadResult(LoadResult result)
    {
        System.Console.WriteLine();
        System.Console.WriteLine($"📊 Load Results:");
        System.Console.WriteLine($"   Status: {(result.IsSuccess ? "✅ Success" : "❌ Failed")}");
        System.Console.WriteLine($"   Processed Records: {result.ProcessedRecords}");
        System.Console.WriteLine($"   Successful Records: {result.SuccessfulRecords}");
        System.Console.WriteLine($"   Failed Records: {result.FailedRecords}");
        System.Console.WriteLine($"   Duration: {result.Duration}");

        if (!string.IsNullOrEmpty(result.Message))
        {
            System.Console.WriteLine($"   Message: {result.Message}");
        }

        if (!string.IsNullOrEmpty(result.ErrorDetails))
        {
            System.Console.WriteLine();
            System.Console.WriteLine("❌ Error Details:");
            System.Console.WriteLine($"   {result.ErrorDetails}");
        }

        if (result.ProcessedRecords > 0)
        {
            var successRate = (double)result.SuccessfulRecords / result.ProcessedRecords * 100;
            System.Console.WriteLine($"   Success Rate: {successRate:F1}%");
        }
    }

    private void DisplayDownloadResult(LoadResult result)
    {
        System.Console.WriteLine();
        System.Console.WriteLine($"📊 Download Results:");
        System.Console.WriteLine($"   Status: {(result.IsSuccess ? "✅ Success" : "❌ Failed")}");
        System.Console.WriteLine($"   Duration: {result.Duration}");

        if (!string.IsNullOrEmpty(result.Message))
        {
            System.Console.WriteLine($"   Message: {result.Message}");
        }

        if (!string.IsNullOrEmpty(result.ErrorDetails))
        {
            System.Console.WriteLine();
            System.Console.WriteLine("❌ Error Details:");
            System.Console.WriteLine($"   {result.ErrorDetails}");
        }
    }

    private async Task DownloadTnoDataAsync(string destination, bool overwrite)
    {
        _logger.LogInformation("Starting TNO test data download");
        _logger.LogInformation("Destination: {Destination}", destination);
        _logger.LogInformation("Overwrite: {Overwrite}", overwrite);

        try
        {
            System.Console.WriteLine($"📥 Downloading TNO test data to {destination}...");
            System.Console.WriteLine();

            var result = await _mediator.Send(new DownloadTnoDataCommand
            {
                DestinationPath = destination,
                OverwriteExisting = overwrite
            });

            DisplayDownloadResult(result);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TNO data download");
            System.Console.WriteLine($"❌ Download operation failed: {ex.Message}");
        }
    }
}
