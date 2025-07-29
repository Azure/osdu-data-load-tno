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
public class DownloadTnoDataCommandHandler : IRequestHandler<DownloadTnoDataCommand, LoadResult>
{
    private readonly ILogger<DownloadTnoDataCommandHandler> _logger;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly OsduConfiguration _configuration;

    public DownloadTnoDataCommandHandler(
        ILogger<DownloadTnoDataCommandHandler> logger,
        IHttpClientFactory httpClientFactory,
        IOptions<OsduConfiguration> configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _httpClientFactory = httpClientFactory ?? throw new ArgumentNullException(nameof(httpClientFactory));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(DownloadTnoDataCommand request, CancellationToken cancellationToken)
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
            
            // Use container temp directory if available (set via TMPDIR/TEMP env vars)
            var tempPath = GetTempDirectoryPath();
            var tempFilePath = Path.Combine(tempPath, "open-test-data.zip");
            _logger.LogInformation("Using temp file: {TempFilePath}", tempFilePath);
            
            try
            {
                using var httpClient = _httpClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(10); // 10 minutes for large downloads
                
                using var response = await httpClient.GetAsync(_configuration.TestDataUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                response.EnsureSuccessStatusCode();

                var totalBytes = response.Content.Headers.ContentLength ?? 0;
                _logger.LogInformation("Download size: {Size:N0} bytes", totalBytes);

                await using var contentStream = await response.Content.ReadAsStreamAsync(cancellationToken);
                await using var fileStream = File.Create(tempFilePath);
                
                var buffer = new byte[81920]; // 80KB buffer
                var totalRead = 0L;
                int bytesRead;
                var lastProgressReport = 0L;
                
                while ((bytesRead = await contentStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                {
                    await fileStream.WriteAsync(buffer, 0, bytesRead, cancellationToken);
                    totalRead += bytesRead;
                    
                    if (totalBytes > 0)
                    {
                        // Report progress every 5MB or every 5% of progress, whichever comes first
                        var progressMB = totalRead / (1024 * 1024);
                        var progress = (double)totalRead / totalBytes * 100;
                        
                        if (progressMB - lastProgressReport >= 5 || (progress > 0 && (int)progress % 5 == 0 && totalRead != lastProgressReport))
                        {
                            _logger.LogInformation("Download progress: {Progress:F1}% ({Downloaded:N0}/{Total:N0} bytes)", 
                                progress, totalRead, totalBytes);
                            lastProgressReport = progressMB;
                        }
                    }
                    else
                    {
                        // If we don't know total size, report every 10MB
                        var progressMB = totalRead / (1024 * 1024);
                        if (progressMB - lastProgressReport >= 10)
                        {
                            _logger.LogInformation("Downloaded: {Downloaded:N0} bytes", totalRead);
                            lastProgressReport = progressMB;
                        }
                    }
                }

                _logger.LogInformation("Download completed: {Size:N0} bytes", totalRead);
            }
            catch (HttpRequestException ex)
            {
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = "Failed to download TNO test data",
                    ErrorDetails = ex.Message,
                    Duration = DateTime.UtcNow - startTime
                };
            }

            // Extract and organize the data
            _logger.LogInformation("Extracting and organizing TNO test data");
            await ExtractAndOrganizeData(tempFilePath, destinationDir, cancellationToken);

            // Clean up temp file
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }

            var duration = DateTime.UtcNow - startTime;
            _logger.LogInformation("TNO test data setup completed in {Duration:mm\\:ss}", duration);

            return new LoadResult
            {
                IsSuccess = true,
                Message = $"TNO test data successfully downloaded and organized in '{destinationDir}'",
                Duration = duration
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during TNO test data download");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Failed to download and setup TNO test data",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }

    private async Task ExtractAndOrganizeData(string zipPath, string destinationDir, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Extracting ZIP archive");
        
        var tempExtractDir = Path.Combine(GetTempDirectoryPath(), $"tno-extract-{Guid.NewGuid()}");
        _logger.LogInformation("Using temp extraction directory: {TempExtractDir}", tempExtractDir);
        
        try
        {
            Directory.CreateDirectory(tempExtractDir);
            
            // Extract ZIP file using streaming approach for large files
            using var archive = ZipFile.OpenRead(zipPath);
            var totalEntries = archive.Entries.Count;
            var extractedEntries = 0;
            
            _logger.LogInformation("Extracting {TotalEntries} files from archive", totalEntries);
            
            foreach (var entry in archive.Entries)
            {
                // Skip directories
                if (string.IsNullOrEmpty(entry.Name))
                    continue;
                
                var entryPath = Path.Combine(tempExtractDir, entry.FullName);
                var entryDir = Path.GetDirectoryName(entryPath);
                
                if (!string.IsNullOrEmpty(entryDir))
                {
                    Directory.CreateDirectory(entryDir);
                }
                
                // Extract file using streaming
                using var entryStream = entry.Open();
                using var fileStream = File.Create(entryPath);
                await entryStream.CopyToAsync(fileStream, cancellationToken);
                
                extractedEntries++;
                
                // Log progress every 100 files
                if (extractedEntries % 100 == 0)
                {
                    var progress = (double)extractedEntries / totalEntries * 100;
                    _logger.LogInformation("Extraction progress: {Progress:F1}% ({Extracted}/{Total} files)", 
                        progress, extractedEntries, totalEntries);
                }
                
                cancellationToken.ThrowIfCancellationRequested();
            }
            
            _logger.LogInformation("Archive extracted: {ExtractedEntries} files", extractedEntries);
            
            // Create the expected directory structure
            CreateDirectoryStructure(destinationDir);
            
            // Find the extracted master directory
            var masterDir = Directory.GetDirectories(tempExtractDir)
                .FirstOrDefault(d => Path.GetFileName(d).Contains("open-test-data"));
            
            if (masterDir == null)
            {
                throw new DirectoryNotFoundException("Could not find extracted open-test-data directory");
            }
            
            // Organize the data according to the expected structure
            OrganizeExtractedData(masterDir, destinationDir, cancellationToken);
            
            _logger.LogInformation("Data organization completed");
        }
        finally
        {
            if (Directory.Exists(tempExtractDir))
            {
                try
                {
                    Directory.Delete(tempExtractDir, true);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to clean up temporary extraction directory: {TempDir}", tempExtractDir);
                }
            }
        }
    }

    private void CreateDirectoryStructure(string destinationDir)
    {
        var datasetDirs = new[]
        {
            "datasets/documents",
            "datasets/markers", 
            "datasets/trajectories",
            "datasets/well-logs",
            "manifests/reference-manifests",
            "manifests/misc-master-data-manifests",
            "manifests/master-well-data-manifests", 
            "manifests/master-wellbore-data-manifests",
            "TNO/provided/TNO/work-products/markers",
            "TNO/provided/TNO/work-products/trajectories",
            "TNO/provided/TNO/work-products/well logs",
            "TNO/provided/TNO/work-products/documents",
            "TNO/contrib",
            "schema",
            "templates"
        };

        foreach (var dir in datasetDirs)
        {
            var fullPath = Path.Combine(destinationDir, dir);
            Directory.CreateDirectory(fullPath);
            _logger.LogInformation("Created directory: {Directory}", dir);
        }
    }

    private void OrganizeExtractedData(string masterDir, string destinationDir, CancellationToken cancellationToken)
    {
        // Map the extracted data to the expected directory structure
        var fileMappings = new Dictionary<string, string>();
        
        // Find and map the different data directories
        var rc100Dir = Path.Combine(masterDir, "rc--1.0.0", "1-data", "3-provided");
        var rc300Dir = Path.Combine(masterDir, "rc--3.0.0");
        
        if (Directory.Exists(rc100Dir))
        {
            // Map file data
            MapDirectoryFiles(Path.Combine(rc100Dir, "USGS_docs"), Path.Combine(destinationDir, "datasets", "documents"), fileMappings);
            MapDirectoryFiles(Path.Combine(rc100Dir, "markers"), Path.Combine(destinationDir, "datasets", "markers"), fileMappings);
            MapDirectoryFiles(Path.Combine(rc100Dir, "trajectories"), Path.Combine(destinationDir, "datasets", "trajectories"), fileMappings);
            MapDirectoryFiles(Path.Combine(rc100Dir, "well-logs"), Path.Combine(destinationDir, "datasets", "well-logs"), fileMappings);
        }
        
        if (Directory.Exists(rc300Dir))
        {
            // Map TNO data
            var tnoProvidedSource = Path.Combine(rc300Dir, "1-data", "3-provided", "TNO");
            var tnoInstancesSource = Path.Combine(rc300Dir, "4-instances", "TNO");
            var schemaSource = Path.Combine(rc300Dir, "3-schema");
            var templatesSource = Path.Combine(rc300Dir, "5-templates");
            
            MapDirectoryFiles(tnoProvidedSource, Path.Combine(destinationDir, "TNO", "contrib"), fileMappings);
            MapDirectoryFiles(tnoInstancesSource, Path.Combine(destinationDir, "TNO", "provided"), fileMappings);
            MapDirectoryFiles(schemaSource, Path.Combine(destinationDir, "schema"), fileMappings);
            MapDirectoryFiles(templatesSource, Path.Combine(destinationDir, "templates"), fileMappings);
        }
        
        // Copy all mapped files
        var totalFiles = fileMappings.Count;
        var processedFiles = 0;
        
        _logger.LogInformation("Organizing {TotalFiles} files", totalFiles);
        
        foreach (var mapping in fileMappings)
        {
            if (File.Exists(mapping.Key))
            {
                var destinationPath = mapping.Value;
                Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);
                
                File.Copy(mapping.Key, destinationPath, true);
                processedFiles++;
                
                // Report progress every 50 files or every 10%
                if (processedFiles % 50 == 0 || (processedFiles % Math.Max(1, totalFiles / 10) == 0))
                {
                    var progress = (double)processedFiles / totalFiles * 100;
                    _logger.LogInformation("File organization progress: {Progress:F1}% ({Processed}/{Total} files)", 
                        progress, processedFiles, totalFiles);
                }
            }
            
            cancellationToken.ThrowIfCancellationRequested();
        }
        
        _logger.LogInformation("Copied {ProcessedFiles} files to destination", processedFiles);
    }

    private void MapDirectoryFiles(string sourceDir, string destDir, Dictionary<string, string> fileMappings)
    {
        if (!Directory.Exists(sourceDir))
        {
            _logger.LogInformation("Source directory does not exist: {SourceDir}", sourceDir);
            return;
        }
        
        var files = Directory.GetFiles(sourceDir, "*", SearchOption.AllDirectories);
        foreach (var file in files)
        {
            var relativePath = Path.GetRelativePath(sourceDir, file);
            var destPath = Path.Combine(destDir, relativePath);
            fileMappings[file] = destPath;
        }
    }

    /// <summary>
    /// Gets the optimal temp directory path, preferring container-mounted directories over system temp
    /// </summary>
    /// <returns>The best available temporary directory path</returns>
    private string GetTempDirectoryPath()
    {
        // Prefer container temp directory (set via TMPDIR/TEMP env vars) over system temp
        var tempPath = Environment.GetEnvironmentVariable("TMPDIR") 
                    ?? Environment.GetEnvironmentVariable("TEMP") 
                    ?? Path.GetTempPath();
        
        _logger.LogInformation("Using temp directory: {TempPath}", tempPath);
        return tempPath;
    }
}
