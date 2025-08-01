using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Identity;
using Azure.Core;
using Azure.Storage.Blobs;

namespace OSDU.DataLoad.Infrastructure.Services;

/// <summary>
/// Consolidated service implementation for all OSDU operations
/// Provides both high-level service operations and low-level HTTP client operations
/// </summary>
public class OsduService : IOsduService, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<OsduService> _logger;
    private readonly OsduConfiguration _configuration;
    private readonly IRetryPolicy _retryPolicy;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly TokenCredential _tokenCredential;
    private string? _accessToken;
    private DateTime _tokenExpiry;

    public OsduService(
        HttpClient httpClient,
        ILogger<OsduService> logger,
        IOptions<OsduConfiguration> configuration,
        IRetryPolicy retryPolicy)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
        _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));

        // Initialize DefaultAzureCredential for authentication
        var options = new DefaultAzureCredentialOptions
        {
            TenantId = _configuration.TenantId
        };

        // If running in Azure with user-assigned managed identity, specify the client ID
        var managedIdentityClientId = Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
        if (!string.IsNullOrEmpty(managedIdentityClientId))
        {
            options.ManagedIdentityClientId = managedIdentityClientId;
            _logger.LogInformation("Using user-assigned managed identity with client ID: {ClientId}", managedIdentityClientId);
        }

        _tokenCredential = new DefaultAzureCredential(options);

        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        ConfigureHttpClient();
    }

    /// <summary>
    /// Service-level method that wraps the raw CreateLegalTagAsync with LoadResult
    /// </summary>
    public async Task<LoadResult> CreateLegalTagAsync(string legalTagName, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating legal tag: {LegalTagName}", legalTagName);
        
        if (!await EnsureAuthenticatedAsync(cancellationToken))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Authentication failed for creating legal tag",
                ProcessedRecords = 1,
                SuccessfulRecords = 0,
                FailedRecords = 1
            };
        }

        try
        {
            var success = await _retryPolicy.ExecuteAsync(async () =>
            {
                var endpoint = $"{_configuration.BaseUrl}/api/legal/v1/legaltags";
                
                var requestData = new
                {
                    name = legalTagName,
                    description = "This tag is used by OSDU TNO Data Load",
                    properties = new
                    {
                        countryOfOrigin = new[] { "US" },
                        contractId = "A1234",
                        expirationDate = "2099-01-25",
                        originator = "MyCompany",
                        dataType = "Transferred Data",
                        securityClassification = "Public",
                        personalData = "No Personal Data",
                        exportClassification = "EAR99"
                    }
                };

                var jsonContent = JsonSerializer.Serialize(requestData, _jsonOptions);
                var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
                content.Headers.Add("data-partition-id", _configuration.DataPartition);

                _logger.LogInformation("Creating legal tag {LegalTagName}", legalTagName);
                
                var response = await _httpClient.PostAsync(endpoint, content, cancellationToken);
                var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation("Successfully created legal tag {LegalTagName}", legalTagName);
                    return true;
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    _logger.LogInformation("Legal tag {LegalTagName} already exists", legalTagName);
                    return true;
                }
                else
                {
                    _logger.LogError("Failed to create legal tag {LegalTagName}. Status: {Status}, Response: {Response}", 
                        legalTagName, response.StatusCode, responseContent);
                    return false;
                }

            }, cancellationToken);

            return new LoadResult
            {
                IsSuccess = success,
                Message = success ? $"Legal tag '{legalTagName}' created successfully" : $"Failed to create legal tag '{legalTagName}'",
                ProcessedRecords = 1,
                SuccessfulRecords = success ? 1 : 0,
                FailedRecords = success ? 0 : 1
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating legal tag: {LegalTagName}", legalTagName);
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Error creating legal tag '{legalTagName}': {ex.Message}",
                ProcessedRecords = 1,
                SuccessfulRecords = 0,
                FailedRecords = 1,
                ErrorDetails = ex.ToString()
            };
        }
    }

    /// <summary>
    /// Submits a workflow request to OSDU (matches Python's send_request function)
    /// This sends the manifest data to the workflow endpoint for processing
    /// </summary>
    public async Task<LoadResult> SubmitWorkflowAsync(object workflowRequest, CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;

        try
        {
            _logger.LogInformation("Submitting workflow request to OSDU");

            return await _retryPolicy.ExecuteAsync(async () =>
            {
                if (!await EnsureAuthenticatedAsync(cancellationToken))
                {
                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = "Authentication failed for creating legal tag",
                        ProcessedRecords = 1,
                        SuccessfulRecords = 0,
                        FailedRecords = 1
                    };
                }

                // Use a custom serializer that preserves exact property names for workflow requests
                var serializeOptions = new JsonSerializerOptions
                {
                    PropertyNamingPolicy = null, // Preserve original property names
                    WriteIndented = false
                };
                var json = JsonSerializer.Serialize(workflowRequest, serializeOptions);
                _logger.LogDebug("Workflow request payload: {Json}", json);

                var content = new StringContent(json, Encoding.UTF8, "application/json");

                // Add required headers for OSDU API
                content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                content.Headers.Add("data-partition-id", _configuration.DataPartition);
                
                // Use the workflow endpoint - matches Python's WORKFLOW_URL
                var workflowUrl = $"{_configuration.BaseUrl.TrimEnd('/')}/api/workflow/v1/workflow/Osdu_ingest/workflowRun";
                _logger.LogDebug("Sending workflow request to: {Url}", workflowUrl);

                var response = await _httpClient.PostAsync(workflowUrl, content, cancellationToken);

                _logger.LogInformation("Workflow submission response: {StatusCode}", response.StatusCode);

                if (response.IsSuccessStatusCode)
                {
                    var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
                    _logger.LogDebug("Workflow response: {Response}", responseContent);

                    // Parse the response to extract runId if available
                    var workflowResponse = JsonSerializer.Deserialize<Dictionary<string, object>>(responseContent);
                    var runId = workflowResponse?.GetValueOrDefault("runId")?.ToString();

                    return new LoadResult
                    {
                        IsSuccess = true,
                        Message = $"Workflow submitted successfully. RunId: {runId}",
                        ProcessedRecords = 1, // One workflow submitted
                        SuccessfulRecords = 1,
                        FailedRecords = 0,
                        Duration = DateTime.UtcNow - startTime,
                        RunId = runId
                    };
                }
                else
                {
                    var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
                    _logger.LogError("Workflow submission failed: {StatusCode} - {ErrorContent}", 
                        response.StatusCode, errorContent.Substring(0, Math.Min(250, errorContent.Length)));

                    return new LoadResult
                    {
                        IsSuccess = false,
                        Message = $"Workflow submission failed with status {response.StatusCode}",
                        ProcessedRecords = 1,
                        SuccessfulRecords = 0,
                        FailedRecords = 1,
                        Duration = DateTime.UtcNow - startTime,
                        ErrorDetails = errorContent
                    };
                }
            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error submitting workflow request to OSDU");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Workflow submission failed",
                ProcessedRecords = 1,
                SuccessfulRecords = 0,
                FailedRecords = 1,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = ex.Message
            };
        }
    }

    /// <summary>
    /// Uploads a single file using SourceFile object
    /// </summary>
    public async Task<FileUploadResult> UploadFileAsync(SourceFile file, CancellationToken cancellationToken = default)
    {
        var correlationId = Guid.NewGuid().ToString("N")[..8]; // Short correlation ID
        var fileSize = 0L;
        
        try
        {
            fileSize = new FileInfo(file.FilePath).Length;
        }
        catch
        {
            // File size calculation failed, will be logged below
        }

        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["CorrelationId"] = correlationId,
            ["FileName"] = file.FileName,
            ["FilePath"] = file.FilePath,
            ["FileSize"] = fileSize
        });

        _logger.LogInformation("[{CorrelationId}] Starting file upload - File: {FileName}, Size: {FileSize} bytes, Path: {FilePath}", 
            correlationId, file.FileName, fileSize, file.FilePath);

        if (!File.Exists(file.FilePath))
        {
            _logger.LogError("[{CorrelationId}] File not found: {FilePath}", correlationId, file.FilePath);
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = $"File not found: {file.FilePath}"
            };
        }

        if (!await EnsureAuthenticatedAsync(cancellationToken))
        {
            _logger.LogError("[{CorrelationId}] Authentication failed for file upload", correlationId);
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = "Authentication failed"
            };
        }

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                // Step 1: Get upload URL from OSDU File API
                _logger.LogInformation("[{CorrelationId}] Step 1: Requesting upload URL from OSDU File API", correlationId);
                var uploadUrlResponse = await GetUploadUrlAsync(correlationId, cancellationToken);
                if (uploadUrlResponse?.Location?.SignedURL == null || uploadUrlResponse.Location.FileSource == null)
                {
                    _logger.LogError("[{CorrelationId}] Step 1 Failed: No upload URL received from OSDU", correlationId);
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Failed to get upload URL from OSDU"
                    };
                }

                _logger.LogInformation("[{CorrelationId}] Step 1 Success: Received upload URL - FileID: {FileID}, FileSource: {FileSource}", 
                    correlationId, uploadUrlResponse.FileID, uploadUrlResponse.Location.FileSource);

                // Step 2: Upload file to Azure Blob Storage
                _logger.LogInformation("[{CorrelationId}] Step 2: Uploading file to Azure Blob Storage", correlationId);
                var blobUploadStart = stopwatch.Elapsed;
                await UploadToBlobStorageAsync(file.FilePath, uploadUrlResponse.Location.SignedURL, correlationId, cancellationToken);
                var blobUploadDuration = stopwatch.Elapsed - blobUploadStart;
                _logger.LogInformation("[{CorrelationId}] Step 2 Success: Blob upload completed in {Duration}ms", 
                    correlationId, blobUploadDuration.TotalMilliseconds);

                // Step 3: Post file metadata to OSDU
                _logger.LogInformation("[{CorrelationId}] Step 3: Creating file metadata record in OSDU", correlationId);
                
                // Ensure we have a valid token before posting metadata
                if (!await EnsureAuthenticatedAsync(cancellationToken))
                {
                    _logger.LogError("[{CorrelationId}] Authentication failed before posting file metadata", correlationId);
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Authentication failed before posting file metadata"
                    };
                }

                var description = $"Uploaded dataset file: {file.FileName}";
                var fileMetadata = CreateFileMetadata(uploadUrlResponse.Location.FileSource, file.FileName, description);
                var metadataResponse = await PostFileMetadataAsync(fileMetadata, correlationId, cancellationToken);
                if (metadataResponse?.Id == null)
                {
                    _logger.LogError("[{CorrelationId}] Step 3 Failed: No file ID received from metadata creation", correlationId);
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Failed to post file metadata to OSDU"
                    };
                }

                _logger.LogInformation("[{CorrelationId}] Step 3 Success: File metadata created - FileID: {FileId}", 
                    correlationId, metadataResponse.Id);

                // Step 4: Get record version from storage API
                _logger.LogInformation("[{CorrelationId}] Step 4: Retrieving record version from storage API - FileID: {FileId}", 
                    correlationId, metadataResponse.Id);
                var recordVersion = await GetRecordVersionAsync(metadataResponse.Id, correlationId, cancellationToken);
                if (recordVersion == null)
                {
                    _logger.LogError("[{CorrelationId}] Step 4 Failed: Could not retrieve record version for FileID: {FileId}", 
                        correlationId, metadataResponse.Id);
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Failed to get record version from OSDU"
                    };
                }

                stopwatch.Stop();
                _logger.LogInformation("[{CorrelationId}] File upload completed successfully - FileID: {FileId}, Version: {Version}, Total Duration: {Duration}ms", 
                    correlationId, metadataResponse.Id, recordVersion, stopwatch.ElapsedMilliseconds);

                return new FileUploadResult
                {
                    IsSuccess = true,
                    Message = "File uploaded successfully",
                    FileId = metadataResponse.Id,
                    FileSource = uploadUrlResponse.Location.FileSource,
                    FileRecordVersion = recordVersion.ToString(),
                    Description = description
                };

            }, cancellationToken);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            _logger.LogError(ex, "[{CorrelationId}] File upload failed after {Duration}ms - File: {FileName}, Error: {ErrorMessage}", 
                correlationId, stopwatch.ElapsedMilliseconds, file.FileName, ex.Message);
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = "File upload operation failed",
                ErrorDetails = ex.Message
            };
        }
    }

    private async Task<UploadUrlResponse?> GetUploadUrlAsync(string correlationId, CancellationToken cancellationToken)
    {
        var endpoint = $"{_configuration.BaseUrl}/api/file/v2/files/uploadURL";
        
        _logger.LogInformation("[{CorrelationId}] Calling GET {Endpoint}", correlationId, endpoint);
        
        var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
        request.Headers.Add("data-partition-id", _configuration.DataPartition);

        var response = await _httpClient.SendAsync(request, cancellationToken);
        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("[{CorrelationId}] GET {Endpoint} failed with status {StatusCode}, response body {responseContent}", 
                correlationId, endpoint, response.StatusCode, responseContent);
            return null;
        }

        _logger.LogInformation("[{CorrelationId}] GET {Endpoint} response: {ResponseContent}", 
            correlationId, endpoint, responseContent);
            
        var uploadLocation = JsonSerializer.Deserialize<UploadUrlResponse>(responseContent, _jsonOptions);
        return uploadLocation;
    }

    private async Task UploadToBlobStorageAsync(string filePath, string signedUrl, string correlationId, CancellationToken cancellationToken)
    {
        _logger.LogInformation("[{CorrelationId}] Uploading to blob storage using signed URL", correlationId);
        
        var blobClient = new BlobClient(new Uri(signedUrl));
        
        using var fileStream = File.OpenRead(filePath);
        var fileSize = fileStream.Length;
        
        _logger.LogInformation("[{CorrelationId}] Starting blob upload - Size: {FileSize} bytes", correlationId, fileSize);
        
        await blobClient.UploadAsync(fileStream, overwrite: true, cancellationToken: cancellationToken);
        
        _logger.LogInformation("[{CorrelationId}] Blob upload completed successfully", correlationId);
    }

    private FileMetadata CreateFileMetadata(string fileSource, string fileName, string description)
    {
        var fileType = Path.GetExtension(fileName)?.TrimStart('.').ToUpperInvariant();
        if (fileType == "LAS")
        {
            fileType = "LAS2";
        }

        return new FileMetadata
        {
            Kind = "osdu:wks:dataset--File.Generic:1.0.0",
            Acl = new Dictionary<string, object>
            {
                ["viewers"] = new[] { _configuration.AclViewer },
                ["owners"] = new[] { _configuration.AclOwner }
            },
            Legal = new Dictionary<string, object>
            {
                ["legaltags"] = new[] { _configuration.LegalTag },
                ["otherRelevantDataCountries"] = new[] { "US" },
                ["status"] = "compliant"
            },
            Data = new Dictionary<string, object>
            {
                ["Description"] = description,
                ["SchemaFormatTypeID"] = $"osdu:reference-data--SchemaFormatType:{fileType}:",
                ["DatasetProperties"] = new Dictionary<string, object>
                {
                    ["FileSourceInfo"] = new Dictionary<string, object>
                    {
                        ["FileSource"] = fileSource,
                        ["Name"] = fileName
                    }
                },
                ["Name"] = fileName
            }
        };
    }

    private async Task<FileMetadataResponse?> PostFileMetadataAsync(FileMetadata metadata, string correlationId, CancellationToken cancellationToken)
    {
        var endpoint = $"{_configuration.BaseUrl}/api/file/v2/files/metadata";
        
        var jsonContent = JsonSerializer.Serialize(metadata, _jsonOptions);
        var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
        content.Headers.Add("data-partition-id", _configuration.DataPartition);

        _logger.LogInformation("[{CorrelationId}] Posting file metadata to {Endpoint}", correlationId, endpoint);
        _logger.LogTrace("[{CorrelationId}] Metadata payload: {Metadata}", correlationId, jsonContent);
        
        var response = await _httpClient.PostAsync(endpoint, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
            _logger.LogError("[{CorrelationId}] POST {Endpoint} failed with status {StatusCode} - Error: {ErrorContent}", 
                correlationId, endpoint, response.StatusCode, errorContent);
            return null;
        }

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        _logger.LogInformation("[{CorrelationId}] POST {Endpoint} response: {ResponseContent}", 
            correlationId, endpoint, responseContent);
            
        var metadataResponse = JsonSerializer.Deserialize<FileMetadataResponse>(responseContent, _jsonOptions);
        return metadataResponse;
    }

    private async Task<long?> GetRecordVersionAsync(string fileId, string correlationId, CancellationToken cancellationToken)
    {
        var endpoint = $"{_configuration.BaseUrl}/api/storage/v2/records/{fileId}";
        
        _logger.LogInformation("[{CorrelationId}] Getting record version from {Endpoint}", correlationId, endpoint);
        
        var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
        request.Headers.Add("data-partition-id", _configuration.DataPartition);

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("[{CorrelationId}] GET {Endpoint} failed with status {StatusCode}", 
                correlationId, endpoint, response.StatusCode);
            return null;
        }

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        _logger.LogInformation("[{CorrelationId}] GET {Endpoint} response: {ResponseContent}", 
            correlationId, endpoint, responseContent);
            
        var versionsResponse = JsonSerializer.Deserialize<StorageVersionsResponse>(responseContent, _jsonOptions);
        
        var version = versionsResponse?.Version;
        _logger.LogInformation("[{CorrelationId}] Retrieved record version: {Version}", correlationId, version);
        
        return version;
    }

    private async Task<bool> EnsureAuthenticatedAsync(CancellationToken cancellationToken = default)
    {
        if (IsTokenValid())
            return true;

        _logger.LogInformation("Authenticating with OSDU platform using DefaultAzureCredential");

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                var tokenRequestContext = new TokenRequestContext(new[] { _configuration.AuthScope });
                var tokenResult = await _tokenCredential.GetTokenAsync(tokenRequestContext, cancellationToken);

                if (string.IsNullOrEmpty(tokenResult.Token))
                {
                    _logger.LogError("Authentication failed: No access token received from DefaultAzureCredential");
                    return false;
                }

                _accessToken = tokenResult.Token;
                _tokenExpiry = tokenResult.ExpiresOn.DateTime.AddMinutes(-5); // 5 minutes buffer

                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);

                _logger.LogInformation("Successfully authenticated with OSDU platform using DefaultAzureCredential");
                return true;

            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to authenticate with OSDU platform using DefaultAzureCredential. Ensure you are logged in with Azure CLI, Visual Studio, or running on Azure with managed identity and have the 'users.datalake.ops' role assigned.");
            return false;
        }
    }

    private bool IsTokenValid()
    {
        return !string.IsNullOrEmpty(_accessToken) && DateTime.UtcNow < _tokenExpiry;
    }

    private void ConfigureHttpClient()
    {
        _httpClient.BaseAddress = new Uri(_configuration.BaseUrl);
        _httpClient.DefaultRequestHeaders.Add("Accept", "application/json");
        _httpClient.DefaultRequestHeaders.Add("User-Agent", "OSDU-DataLoad-TNO/1.0");
        // Timeout is configured at the HttpClient registration level (15 minutes)
    }

    public void Dispose()
    {
        _httpClient?.Dispose();
    }

    private class UploadResponse
    {
        public int RecordCount { get; set; }
        public string[]? FailedRecordIds { get; set; }
    }
}
