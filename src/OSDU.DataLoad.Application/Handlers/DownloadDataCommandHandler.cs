using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using System.IO.Compression;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for downloading and setting up TNO test data
/// </summary>
public class DownloadDataCommandHandler : IRequestHandler<DownloadDataCommand, LoadResult>
{
    private readonly ILogger<DownloadDataCommandHandler> _logger;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly OsduConfiguration _configuration;

    public DownloadDataCommandHandler(
        ILogger<DownloadDataCommandHandler> logger,
        IHttpClientFactory httpClientFactory,
        IOptions<OsduConfiguration> configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(DownloadDataCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting TNO test data download to {DestinationPath}", request.DestinationPath);

        try
        {
            // Validate destination path
            if (string.IsNullOrWhiteSpace(request.DestinationPath))
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = "Destination path is required",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            var destinationDir = Path.GetFullPath(request.DestinationPath);
            
            // Check if destination already exists
            if (Directory.Exists(destinationDir) && !request.OverwriteExisting)
            {
                var files = Directory.GetFiles(destinationDir, "*", SearchOption.AllDirectories);
                if (files.Length > 0)
                {
                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = $"Destination directory '{destinationDir}' already exists and is not empty. Use --overwrite to replace existing data.",
                        Duration = DateTime.UtcNow - startTime
                    };
                }
            }

            // Create destination directory
            Directory.CreateDirectory(destinationDir);

            // Download the zip file
            _logger.LogInformation("Downloading TNO test data from {Url}", _configuration.TestDataUrl);
            
            var tempPath = Path.GetTempPath();
            var tempFilePath = Path.Combine(tempPath, "open-test-data.zip");
            _logger.LogInformation("Using temp file: {TempFilePath}", tempFilePath);
            
            try
            {
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(10);
                
                using var response = await httpClient.GetAsync(_configuration.TestDataUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                response.EnsureSuccessStatusCode();

                var totalBytes = response.Content.Headers.ContentLength ?? 0;
                _logger.LogInformation("Download size: {Size:N0} bytes", totalBytes);

                await using var contentStream = await response.Content.ReadAsStreamAsync(cancellationToken);
                await using var fileStream = File.Create(tempFilePath);
                
                var buffer = new byte[81920];
                var totalRead = 0L;
                int bytesRead;
                var lastProgressReport = 0L;
                
                while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                {
                    await fileStream.WriteAsync(buffer, 0, bytesRead, cancellationToken);
                    totalRead += bytesRead;
                    
                    if (totalBytes > 0 && totalRead - lastProgressReport > totalBytes / 10)
                    {
                        var progress = (double)totalRead / totalBytes * 100;
                        _logger.LogInformation("Download progress: {Progress:F1}%", progress);
                        lastProgressReport = totalRead;
                    }
                }

                _logger.LogInformation("Download completed, extracting archive");

                // Extract the zip file
                using var archive = ZipFile.OpenRead(tempFilePath);
                
                var rootEntry = archive.Entries.FirstOrDefault(e => 
                    e.FullName.StartsWith("open-test-data-master/") && 
                    e.FullName.Contains("TNO/") && 
                    !string.IsNullOrEmpty(e.Name));

                if (rootEntry == null)
                {
                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = "TNO data not found in the downloaded archive",
                        Duration = DateTime.UtcNow - startTime
                    };
                }

                var extractedCount = 0;
                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.StartsWith("open-test-data-master/"))
                    {
                        var relativePath = entry.FullName.Substring("open-test-data-master/".Length);
                        var destPath = Path.Combine(destinationDir, relativePath);

                        if (string.IsNullOrEmpty(entry.Name))
                        {
                            Directory.CreateDirectory(destPath);
                        }
                        else
                        {
                            Directory.CreateDirectory(Path.GetDirectoryName(destPath)!);
                            entry.ExtractToFile(destPath, true);
                            extractedCount++;
                        }
                    }
                }

                _logger.LogInformation("Extracted {Count} files successfully", extractedCount);

                return new LoadResult
                {
                    IsSuccess = true,
                    Message = $"TNO test data downloaded and extracted successfully to {destinationDir}",
                    ProcessedRecords = extractedCount,
                    SuccessfulRecords = extractedCount,
                    Duration = DateTime.UtcNow - startTime
                };
            }
            finally
            {
                // Clean up temp file
                if (File.Exists(tempFilePath))
                {
                    try
                    {
                        File.Delete(tempFilePath);
                        _logger.LogDebug("Cleaned up temp file: {TempFilePath}", tempFilePath);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete temp file: {TempFilePath}", tempFilePath);
                    }
                }
            }
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "HTTP error during download");
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Download failed: {ex.Message}",
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.ToString()
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TNO data download");
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Download failed: {ex.Message}",
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.ToString()
            };
        }
    }
}
