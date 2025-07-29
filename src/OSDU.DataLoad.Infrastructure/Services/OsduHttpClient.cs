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

    public async Task<bool> AuthenticateAsync(CancellationToken cancellationToken = default)
    {
        if (IsTokenValid())
        {
            _logger.LogInformation("Using existing valid access token");
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

                    // Get fresh token for each batch to avoid expiration
                    _accessToken = null;
                    if (!await EnsureAuthenticatedAsync(cancellationToken))
                    {
                        _logger.LogError("Authentication failed before batch {BatchNumber}", (i / _configuration.BatchSize) + 1);
                        aggregateResult.IsSuccess = false;
                        aggregateResult.Message = "Authentication failed during batch processing";
                        aggregateResult.FailedRecords += batch.Length;
                        continue;
                    }

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
            
            _logger.LogInformation("Successfully uploaded batch: {SuccessCount} records, {FailCount} failed",
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

    public async Task<bool> AddUserToOpsGroupAsync(string dataPartition, string userEmail, CancellationToken cancellationToken = default)
    {
        if (!await EnsureAuthenticatedAsync(cancellationToken))
        {
            _logger.LogError("Authentication failed for adding user to ops group");
            return false;
        }

        var groupName = $"users.datalake.ops@{dataPartition}.dataservices.energy";

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
            {
                var endpoint = $"{_configuration.BaseUrl}/api/entitlements/v2/groups/{groupName}/members";
                
                var requestData = new
                {
                    email = userEmail,
                    role = "MEMBER"
                };

                var jsonContent = JsonSerializer.Serialize(requestData, _jsonOptions);
                var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
                content.Headers.Add("data-partition-id", _configuration.DataPartition);

                _logger.LogInformation("Adding user {UserEmail} to ops group {GroupName}", 
                    userEmail, groupName);
                
                var response = await _httpClient.PostAsync(endpoint, content, cancellationToken);
                var responseContent = await response.Content.ReadAsStringAsync(cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogInformation("Successfully added user {UserEmail} to ops group {GroupName}", 
                        userEmail, groupName);
                    return true;
                }
                else if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    _logger.LogInformation("User {UserEmail} is already a member of ops group {GroupName}", 
                        userEmail, groupName);
                    return true;
                }
                else
                {
                    _logger.LogError("Failed to add user {UserEmail} to ops group {GroupName}. Status: {Status}, Response: {Response}", 
                        userEmail, groupName, response.StatusCode, responseContent);
                    return false;
                }

            }, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adding user {UserEmail} to ops group {GroupName}", userEmail, groupName);
            return false;
        }
    }

    public async Task<bool> CreateLegalTagAsync(string legalTagName, CancellationToken cancellationToken = default)
    {
        if (!await EnsureAuthenticatedAsync(cancellationToken))
        {
            _logger.LogError("Authentication failed for creating legal tag");
            return false;
        }

        try
        {
            return await _retryPolicy.ExecuteAsync(async () =>
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
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating legal tag {LegalTagName}", legalTagName);
            return false;
        }
    }

    public async Task<FileUploadResult> UploadFileAsync(string filePath, string fileName, string description, CancellationToken cancellationToken = default)
    {
        var correlationId = Guid.NewGuid().ToString("N")[..8]; // Short correlation ID
        var fileSize = 0L;
        
        try
        {
            fileSize = new FileInfo(filePath).Length;
        }
        catch
        {
            // File size calculation failed, will be logged below
        }

        using var scope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["CorrelationId"] = correlationId,
            ["FileName"] = fileName,
            ["FilePath"] = filePath,
            ["FileSize"] = fileSize
        });

        _logger.LogInformation("[{CorrelationId}] Starting file upload - File: {FileName}, Size: {FileSize} bytes, Path: {FilePath}", 
            correlationId, fileName, fileSize, filePath);

        if (!File.Exists(filePath))
        {
            _logger.LogError("[{CorrelationId}] File not found: {FilePath}", correlationId, filePath);
            return new FileUploadResult
            {
                IsSuccess = false,
                Message = $"File not found: {filePath}"
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
                await UploadToBlobStorageAsync(filePath, uploadUrlResponse.Location.SignedURL, correlationId, cancellationToken);
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

                var fileMetadata = CreateFileMetadata(uploadUrlResponse.Location.FileSource, fileName, description);
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
                correlationId, stopwatch.ElapsedMilliseconds, fileName, ex.Message);
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
