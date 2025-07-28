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
/// HTTP client implementation for OSDU API operations
/// </summary>
public class OsduHttpClient : IOsduClient, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<OsduHttpClient> _logger;
    private readonly OsduConfiguration _configuration;
    private readonly IRetryPolicy _retryPolicy;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly TokenCredential _tokenCredential;
    private string? _accessToken;
    private DateTime _tokenExpiry;

    public OsduHttpClient(
        HttpClient httpClient,
        ILogger<OsduHttpClient> logger,
        IOptions<OsduConfiguration> configuration,
        IRetryPolicy retryPolicy)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
        _retryPolicy = retryPolicy ?? throw new ArgumentNullException(nameof(retryPolicy));

        // Initialize DefaultAzureCredential for authentication
        _tokenCredential = new DefaultAzureCredential(new DefaultAzureCredentialOptions
        {
            TenantId = _configuration.TenantId
        });

        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        ConfigureHttpClient();
    }

    public async Task<bool> AuthenticateAsync(CancellationToken cancellationToken = default)
    {
        if (IsTokenValid())
        {
            _logger.LogDebug("Using existing valid access token");
            return true;
        }

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

    public async Task<LoadResult> UploadRecordsAsync(IEnumerable<DataRecord> records, CancellationToken cancellationToken = default)
    {
        var recordArray = records.ToArray();
        _logger.LogInformation("Uploading {RecordCount} records to OSDU in batches of {BatchSize}", recordArray.Length, _configuration.BatchSize);

        if (!await EnsureAuthenticatedAsync(cancellationToken))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Authentication failed",
                ProcessedRecords = recordArray.Length,
                FailedRecords = recordArray.Length
            };
        }

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                var aggregateResult = new LoadResult
                {
                    IsSuccess = true,
                    Message = "Records uploaded successfully",
                    ProcessedRecords = recordArray.Length,
                    SuccessfulRecords = 0,
                    FailedRecords = 0
                };

                // Split records into batches
                for (int i = 0; i < recordArray.Length; i += _configuration.BatchSize)
                {
                    var batch = recordArray.Skip(i).Take(_configuration.BatchSize).ToArray();
                    _logger.LogInformation("Uploading batch {BatchNumber}/{TotalBatches} ({RecordCount} records)", 
                        (i / _configuration.BatchSize) + 1, 
                        (recordArray.Length + _configuration.BatchSize - 1) / _configuration.BatchSize,
                        batch.Length);

                    var batchResult = await UploadRecordBatchAsync(batch, cancellationToken);
                    
                    // Aggregate results
                    aggregateResult.SuccessfulRecords += batchResult.SuccessfulRecords;
                    aggregateResult.FailedRecords += batchResult.FailedRecords;

                    if (!batchResult.IsSuccess)
                    {
                        aggregateResult.IsSuccess = false;
                        aggregateResult.Message = "Some batches failed to upload";
                        if (string.IsNullOrEmpty(aggregateResult.ErrorDetails))
                        {
                            aggregateResult.ErrorDetails = batchResult.ErrorDetails;
                        }
                        else
                        {
                            aggregateResult.ErrorDetails += "; " + batchResult.ErrorDetails;
                        }
                    }
                }

                _logger.LogInformation("Completed batch upload: {SuccessCount} successful, {FailCount} failed out of {TotalCount} records",
                    aggregateResult.SuccessfulRecords, aggregateResult.FailedRecords, recordArray.Length);

                return aggregateResult;

            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error uploading records to OSDU");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Upload operation failed",
                ErrorDetails = ex.Message,
                ProcessedRecords = recordArray.Length,
                FailedRecords = recordArray.Length
            };
        }
    }

    private async Task<LoadResult> UploadRecordBatchAsync(DataRecord[] batch, CancellationToken cancellationToken)
    {      
        var jsonContent = JsonSerializer.Serialize(batch, _jsonOptions);
        var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");

        // Add required OSDU headers
        content.Headers.Add("data-partition-id", _configuration.DataPartition);

        var endpoint = $"{_configuration.BaseUrl}/api/storage/v2/records";
        var response = await _httpClient.PutAsync(endpoint, content, cancellationToken);

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

        if (response.IsSuccessStatusCode)
        {
            var uploadResponse = JsonSerializer.Deserialize<UploadResponse>(responseContent, _jsonOptions);
            
            _logger.LogDebug("Successfully uploaded batch: {SuccessCount} records, {FailCount} failed",
                uploadResponse?.RecordCount ?? batch.Length, 
                uploadResponse?.FailedRecordIds?.Length ?? 0);

            return new LoadResult
            {
                IsSuccess = true,
                Message = "Batch uploaded successfully",
                ProcessedRecords = batch.Length,
                SuccessfulRecords = (uploadResponse?.RecordCount ?? batch.Length) - (uploadResponse?.FailedRecordIds?.Length ?? 0),
                FailedRecords = uploadResponse?.FailedRecordIds?.Length ?? 0
            };
        }
        else
        {
            _logger.LogError("Failed to upload batch. Status: {Status}, Response: {Response}", 
                response.StatusCode, responseContent);

            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Batch upload failed with status: {response.StatusCode}",
                ErrorDetails = responseContent,
                ProcessedRecords = batch.Length,
                FailedRecords = batch.Length
            };
        }
    }

    public async Task<bool> RecordExistsAsync(string recordId, CancellationToken cancellationToken = default)
    {
        if (!await EnsureAuthenticatedAsync(cancellationToken))
            return false;

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                var endpoint = $"{_configuration.BaseUrl}/api/storage/v2/records/{recordId}";
                
                var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                request.Headers.Add("data-partition-id", _configuration.DataPartition);

                var response = await _httpClient.SendAsync(request, cancellationToken);
                return response.IsSuccessStatusCode;

            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if record exists: {RecordId}", recordId);
            return false;
        }
    }

    public async Task<string> GetSchemaAsync(string kind, CancellationToken cancellationToken = default)
    {
        if (!await EnsureAuthenticatedAsync(cancellationToken))
            return string.Empty;

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                var endpoint = $"{_configuration.BaseUrl}/api/schema-service/v1/schema/{kind}";
                
                var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                request.Headers.Add("data-partition-id", _configuration.DataPartition);

                var response = await _httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();

                return await response.Content.ReadAsStringAsync(cancellationToken);

            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error getting schema for kind: {Kind}", kind);
            return string.Empty;
        }
    }

    public async Task<ValidationResult[]> ValidateRecordsAsync(IEnumerable<DataRecord> records, CancellationToken cancellationToken = default)
    {
        var recordArray = records.ToArray();
        _logger.LogInformation("Validating {RecordCount} records", recordArray.Length);

        // For now, implement basic validation
        // In a real implementation, this would call OSDU validation APIs
        await Task.CompletedTask;

        return recordArray.Select(record => new ValidationResult
        {
            IsValid = !string.IsNullOrEmpty(record.Id) && !string.IsNullOrEmpty(record.Kind),
            Errors = GetValidationErrors(record),
            RecordId = record.Id
        }).ToArray();
    }

    public async Task<FileUploadResult> UploadFileAsync(string filePath, string fileName, string description, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting upload for file: {FilePath}", filePath);

        if (!File.Exists(filePath))
        {
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = $"File not found: {filePath}"
            };
        }

        if (!await EnsureAuthenticatedAsync(cancellationToken))
        {
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = "Authentication failed"
            };
        }

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                // Step 1: Get upload URL from OSDU File API
                _logger.LogDebug("Requesting upload URL for: {FilePath}", filePath);
                var uploadUrlResponse = await GetUploadUrlAsync(cancellationToken);
                if (uploadUrlResponse?.Location?.SignedURL == null || uploadUrlResponse.Location.FileSource == null)
                {
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Failed to get upload URL from OSDU"
                    };
                }

                _logger.LogDebug("Received signed URL for {FilePath}", filePath);

                // Step 2: Upload file to Azure Blob Storage
                _logger.LogDebug("Uploading file to blob: {FilePath}", filePath);
                await UploadToBlobStorageAsync(filePath, uploadUrlResponse.Location.SignedURL, cancellationToken);
                _logger.LogDebug("Blob upload completed for {FilePath}", filePath);

                // Step 3: Post file metadata to OSDU
                _logger.LogDebug("Populating metadata for {FilePath}", filePath);
                var fileMetadata = CreateFileMetadata(uploadUrlResponse.Location.FileSource, fileName, description);
                var metadataResponse = await PostFileMetadataAsync(fileMetadata, cancellationToken);
                if (metadataResponse?.Id == null)
                {
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Failed to post file metadata to OSDU"
                    };
                }

                // Step 4: Get record version from storage API
                _logger.LogDebug("Getting record version for {FilePath} with file ID: {FileId}", filePath, metadataResponse.Id);
                var recordVersion = await GetRecordVersionAsync(metadataResponse.Id, cancellationToken);
                if (recordVersion == null)
                {
                    return new FileUploadResult
                    {
                        IsSuccess = false,
                        Message = "Failed to get record version from OSDU"
                    };
                }

                _logger.LogInformation("File upload completed: {FileName}", fileName);

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
            _logger.LogError(ex, "File Upload Failed: {FileName} Reason: {Reason}", fileName, ex.Message);
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = "File upload operation failed",
                ErrorDetails = ex.Message
            };
        }
    }

    private async Task<UploadUrlResponse?> GetUploadUrlAsync(CancellationToken cancellationToken)
    {
        var endpoint = $"{_configuration.BaseUrl}/api/file/v2/files/uploadURL";
        
        var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
        request.Headers.Add("data-partition-id", _configuration.DataPartition);

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("/files/uploadURL failed with response {StatusCode}", response.StatusCode);
            return null;
        }

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        return JsonSerializer.Deserialize<UploadUrlResponse>(responseContent, _jsonOptions);
    }

    private async Task UploadToBlobStorageAsync(string filePath, string signedUrl, CancellationToken cancellationToken)
    {
        var blobClient = new BlobClient(new Uri(signedUrl));
        
        using var fileStream = File.OpenRead(filePath);
        await blobClient.UploadAsync(fileStream, overwrite: true, cancellationToken: cancellationToken);
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

    private async Task<FileMetadataResponse?> PostFileMetadataAsync(FileMetadata metadata, CancellationToken cancellationToken)
    {
        var endpoint = $"{_configuration.BaseUrl}/api/file/v2/files/metadata";
        
        var jsonContent = JsonSerializer.Serialize(metadata, _jsonOptions);
        var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
        content.Headers.Add("data-partition-id", _configuration.DataPartition);

        _logger.LogInformation("Posting file metadata: {Metadata}", jsonContent);
        var response = await _httpClient.PostAsync(endpoint, content, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
            _logger.LogError("/files/metadata failed with response {StatusCode} and body {ErrorContent}", 
                response.StatusCode, errorContent);
            return null;
        }

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        return JsonSerializer.Deserialize<FileMetadataResponse>(responseContent, _jsonOptions);
    }

    private async Task<long?> GetRecordVersionAsync(string fileId, CancellationToken cancellationToken)
    {
        var endpoint = $"{_configuration.BaseUrl}/api/storage/v2/records/{fileId}/versions";
        
        var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
        request.Headers.Add("data-partition-id", _configuration.DataPartition);

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            _logger.LogError("/storage/versions failed for file ID {FileId} with response {StatusCode}", 
                fileId, response.StatusCode);
            return null;
        }

        var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);
        var versionsResponse = JsonSerializer.Deserialize<StorageVersionsResponse>(responseContent, _jsonOptions);
        
        return versionsResponse?.Versions?.FirstOrDefault();
    }

    private string[] GetValidationErrors(DataRecord record)
    {
        var errors = new List<string>();

        if (string.IsNullOrEmpty(record.Id))
            errors.Add("Record ID is required");

        if (string.IsNullOrEmpty(record.Kind))
            errors.Add("Record kind is required");

        if (record.Data.Count == 0)
            errors.Add("Record data is required");

        if (record.Legal.Count == 0)
            errors.Add("Legal information is required");

        if (record.Acl.Count == 0)
            errors.Add("ACL information is required");

        return errors.ToArray();
    }

    private async Task<bool> EnsureAuthenticatedAsync(CancellationToken cancellationToken = default)
    {
        if (IsTokenValid())
            return true;

        return await AuthenticateAsync(cancellationToken);
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
        _httpClient.Timeout = TimeSpan.FromMinutes(10);
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
