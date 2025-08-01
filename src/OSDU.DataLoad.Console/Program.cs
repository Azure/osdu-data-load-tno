using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using OSDU.DataLoad.Domain.Interfaces;
using OSDU.DataLoad.Infrastructure.Services;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Console;

/// <summary>
/// Main entry point for the OSDU Data Load console application
/// </summary>
public class Program
{
    public static async Task<int> Main(string[] args)
    {
        IHost? host = null;
        
        try
        {
            host = CreateHostBuilder(args).Build();
            
            using var scope = host.Services.CreateScope();
            var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
            
            logger.LogInformation("OSDU Data Load TNO starting - Process ID: {ProcessId}, Args: [{Args}]", 
                Environment.ProcessId, string.Join(", ", args));
            
            // Set up global exception handlers for container resilience
            AppDomain.CurrentDomain.UnhandledException += (sender, e) =>
            {
                logger.LogCritical("Unhandled exception occurred: {Exception}", e.ExceptionObject);
            };
            
            TaskScheduler.UnobservedTaskException += (sender, e) =>
            {
                logger.LogError("Unobserved task exception: {Exception}", e.Exception);
                e.SetObserved(); // Prevent process termination
            };
            
            var app = scope.ServiceProvider.GetRequiredService<DataLoadApplication>();
            
            var result = await app.RunAsync(args);
            
            logger.LogInformation("OSDU Data Load TNO completed with exit code: {ExitCode}", result);
            return result;
        }
        catch (OperationCanceledException ex)
        {
            // Handle cancellation gracefully (e.g., container shutdown)
            var logger = host?.Services?.GetService<ILogger<Program>>();
            logger?.LogWarning("Operation was cancelled - likely due to container shutdown: {Message}", ex.Message);
            return 130; // Standard exit code for SIGINT
        }
        catch (Exception ex)
        {
            // Prevent container crash by catching all exceptions
            var logger = host?.Services?.GetService<ILogger<Program>>();
            logger?.LogCritical(ex, "Fatal error occurred - preventing container crash: {Message}", ex.Message);
            
            // Return non-zero exit code but don't let the process crash
            return 1;
        }
        finally
        {
            try
            {
                host?.Dispose();
            }
            catch (Exception ex)
            {
                // Even disposal should not crash the container
                var logger = host?.Services?.GetService<ILogger<Program>>();
                logger?.LogError(ex, "Error during cleanup: {Message}", ex.Message);
            }
        }
    }

    private static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration((context, config) =>
            {
                config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                config.AddJsonFile($"appsettings.{context.HostingEnvironment.EnvironmentName}.json", optional: true);
                config.AddEnvironmentVariables("OSDU_");
                config.AddCommandLine(args);
            })
            .ConfigureServices((context, services) =>
            {
                // Configuration - use proper environment variable mapping
                services.Configure<OsduConfiguration>(osduConfig =>
                {
                    // First bind from Osdu section in appsettings.json
                    context.Configuration.GetSection("Osdu").Bind(osduConfig);
                    
                    // Then override with environment variables if they exist
                    if (!string.IsNullOrEmpty(context.Configuration["BaseUrl"]))
                        osduConfig.BaseUrl = context.Configuration["BaseUrl"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["ClientId"]))
                        osduConfig.ClientId = context.Configuration["ClientId"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["TenantId"]))
                        osduConfig.TenantId = context.Configuration["TenantId"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["DataPartition"]))
                        osduConfig.DataPartition = context.Configuration["DataPartition"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["LegalTag"]))
                        osduConfig.LegalTag = context.Configuration["LegalTag"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["AclViewer"]))
                        osduConfig.AclViewer = context.Configuration["AclViewer"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["AclOwner"]))
                        osduConfig.AclOwner = context.Configuration["AclOwner"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["UserEmail"]))
                        osduConfig.UserEmail = context.Configuration["UserEmail"]!;
                    if (!string.IsNullOrEmpty(context.Configuration["TestDataUrl"]))
                        osduConfig.TestDataUrl = context.Configuration["TestDataUrl"]!;
                });

                // Path configuration - centralized file and directory paths
                services.AddSingleton<PathConfiguration>(provider =>
                {
                    var basePath = context.Configuration["BasePath"] ?? 
                                   Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "osdu-data", "tno");
                    return new PathConfiguration { BaseDataPath = basePath };
                });

                // MediatR
                services.AddMediatR(cfg => {
                    cfg.RegisterServicesFromAssembly(typeof(OSDU.DataLoad.Application.Commands.CreateLegalTagCommand).Assembly);
                });

                // Domain services
                services.AddScoped<IDataTransformer, TnoDataTransformer>();
                services.AddScoped<IFileProcessor, FileProcessor>();
                services.AddScoped<IManifestGenerator, ManifestGenerator>();
                services.AddScoped<IRetryPolicy, ExponentialRetryPolicy>();
                
                // Progress reporting services
                services.AddScoped<IManifestProgressReporter, ManifestProgressReporter>();

                // Consolidated OSDU service with direct HTTP client
                services.AddHttpClient<IOsduService, OsduService>()
                    .ConfigureHttpClient(client =>
                    {
                        // Container-friendly timeouts
                        client.Timeout = TimeSpan.FromMinutes(15); // Longer timeout for large uploads
                    })
                    .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler()
                    {
                        // Enable retries and connection pooling for container resilience
                        MaxConnectionsPerServer = 10,
                    });
                    
                services.AddHttpClient(); // For general HTTP operations
                
                // Application
                services.AddScoped<DataLoadApplication>();
            })
            .ConfigureLogging((context, logging) =>
            {
                logging.ClearProviders();
                
                // Container-friendly logging
                logging.AddSimpleConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
                });
                
                // Add structured logging for container environments
                logging.AddJsonConsole(options =>
                {
                    options.IncludeScopes = true;
                    options.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";
                });
                
                if (context.HostingEnvironment.IsDevelopment())
                {
                    logging.AddDebug();
                    logging.SetMinimumLevel(LogLevel.Debug);
                }
                else
                {
                    // Production/container environment
                    logging.SetMinimumLevel(LogLevel.Information);
                }
                
                // Always log critical errors regardless of environment
                logging.AddFilter("OSDU.DataLoad", LogLevel.Information);
                logging.AddFilter("Microsoft", LogLevel.Warning);
                logging.AddFilter("System", LogLevel.Warning);
            });
}
