using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using OSDU.DataLoad.Domain.Interfaces;
using OSDU.DataLoad.Application.Services;
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
        try
        {
            var host = CreateHostBuilder(args).Build();
            
            using var scope = host.Services.CreateScope();
            var app = scope.ServiceProvider.GetRequiredService<DataLoadApplication>();
            
            return await app.RunAsync(args);
        }
        catch (Exception ex)
        {
            System.Console.WriteLine($"Fatal error: {ex.Message}");
            return -1;
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
                    if (!string.IsNullOrEmpty(context.Configuration["TestDataUrl"]))
                        osduConfig.TestDataUrl = context.Configuration["TestDataUrl"]!;
                });

                // MediatR
                services.AddMediatR(cfg => {
                    cfg.RegisterServicesFromAssembly(typeof(OSDU.DataLoad.Application.Commands.LoadDataCommand).Assembly);
                });

                // Domain services
                services.AddScoped<IDataTransformer, TnoDataTransformer>();
                services.AddScoped<IFileProcessor, FileProcessor>();
                services.AddScoped<IManifestGenerator, ManifestGenerator>();
                services.AddScoped<IRetryPolicy, ExponentialRetryPolicy>();

                // Infrastructure services
                services.AddHttpClient<IOsduClient, OsduHttpClient>();
                services.AddHttpClient(); // For general HTTP operations
                
                // Application
                services.AddScoped<DataLoadApplication>();
            })
            .ConfigureLogging((context, logging) =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.AddDebug();
                
                if (context.HostingEnvironment.IsDevelopment())
                {
                    logging.SetMinimumLevel(LogLevel.Debug);
                }
                else
                {
                    logging.SetMinimumLevel(LogLevel.Information);
                }
            });
}
