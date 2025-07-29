using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace OSDU.DataLoad.Infrastructure.Services;

/// <summary>
/// Data transformer for converting TNO data to OSDU format
/// </summary>
public class TnoDataTransformer : IDataTransformer
{
    private readonly ILogger<TnoDataTransformer> _logger;
    private readonly OsduConfiguration _configuration;
    private readonly JsonSerializerOptions _jsonOptions;

    public TnoDataTransformer(ILogger<TnoDataTransformer> logger, IOptions<OsduConfiguration> configuration)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
    }

    public async Task<DataRecord[]> TransformAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Transforming {DataType} data from {FileName}", dataType, sourceFile.FileName);

        if (!File.Exists(sourceFile.FilePath))
        {
            throw new FileNotFoundException($"Source file not found: {sourceFile.FilePath}");
        }

        try
        {
            var records = new List<DataRecord>();

            switch (sourceFile.FileType.ToLowerInvariant())
            {
                case "csv":
                    records.AddRange(await TransformCsvAsync(sourceFile, dataType, cancellationToken));
                    break;
                case "json":
                    records.AddRange(await TransformJsonAsync(sourceFile, dataType, cancellationToken));
                    break;
                case "xlsx":
                case "xls":
                    records.AddRange(await TransformExcelAsync(sourceFile, dataType, cancellationToken));
                    break;
                case "las":
                    records.AddRange(await TransformLasAsync(sourceFile, dataType, cancellationToken));
                    break;
                case "dlis":
                    records.AddRange(await TransformDlisAsync(sourceFile, dataType, cancellationToken));
                    break;
                case "pdf":
                case "doc":
                case "docx":
                case "txt":
                    records.AddRange(await TransformDocumentAsync(sourceFile, dataType, cancellationToken));
                    break;
                default:
                    throw new NotSupportedException($"File type {sourceFile.FileType} is not supported for {dataType}");
            }

            _logger.LogInformation("Transformed {RecordCount} records from {FileName}", records.Count, sourceFile.FileName);
            return records.ToArray();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error transforming data from {FileName}", sourceFile.FileName);
            throw;
        }
    }

    public async Task<ValidationResult> ValidateSourceAsync(SourceFile sourceFile, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Validating source data in {FileName}", sourceFile.FileName);

        var errors = new List<string>();
        var warnings = new List<string>();

        try
        {
            if (!File.Exists(sourceFile.FilePath))
            {
                errors.Add("Source file does not exist");
                return new ValidationResult
                {
                    IsValid = false,
                    Errors = errors.ToArray(),
                    Warnings = warnings.ToArray()
                };
            }

            var content = await File.ReadAllTextAsync(sourceFile.FilePath, cancellationToken);
            
            if (string.IsNullOrWhiteSpace(content))
            {
                errors.Add("Source file is empty");
            }

            // Validate based on file type
            switch (sourceFile.FileType.ToLowerInvariant())
            {
                case "csv":
                    ValidateCsvContent(content, errors, warnings);
                    break;
                case "json":
                    ValidateJsonContent(content, errors, warnings);
                    break;
                case "xlsx":
                case "xls":
                    // Excel validation would require additional libraries
                    warnings.Add("Excel validation is limited");
                    break;
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Error validating source: {ex.Message}");
        }

        var isValid = errors.Count == 0;

        _logger.LogInformation("Source validation completed for {FileName}. Valid: {IsValid}, Errors: {ErrorCount}, Warnings: {WarningCount}",
            sourceFile.FileName, isValid, errors.Count, warnings.Count);

        return new ValidationResult
        {
            IsValid = isValid,
            Errors = errors.ToArray(),
            Warnings = warnings.ToArray()
        };
    }

    public string[] GetSupportedExtensions(TnoDataType dataType)
    {
        return dataType switch
        {
            TnoDataType.Wells => new[] { "csv", "json", "xlsx" },
            TnoDataType.Wellbores => new[] { "csv", "json", "xlsx" },
            TnoDataType.WellboreTrajectories => new[] { "csv", "json", "las" },
            TnoDataType.WellMarkers => new[] { "csv", "json", "xlsx" },
            TnoDataType.WellboreMarkers => new[] { "csv", "json", "xlsx" },
            TnoDataType.WellLogs => new[] { "las", "dlis", "csv", "json" },
            TnoDataType.ReferenceData => new[] { "csv", "json", "xlsx" },
            TnoDataType.Horizons => new[] { "csv", "json", "txt" },
            TnoDataType.Formations => new[] { "csv", "json", "xlsx" },
            TnoDataType.WellCompletions => new[] { "csv", "json", "xlsx" },
            _ => new[] { "csv", "json" }
        };
    }

    private async Task<DataRecord[]> TransformCsvAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken)
    {
        var lines = await File.ReadAllLinesAsync(sourceFile.FilePath, cancellationToken);
        
        if (lines.Length < 2) // Header + at least one data row
        {
            return Array.Empty<DataRecord>();
        }

        var headers = lines[0].Split(',');
        var records = new List<DataRecord>();

        for (int i = 1; i < lines.Length; i++)
        {
            var values = lines[i].Split(',');
            
            if (values.Length != headers.Length)
            {
                _logger.LogWarning("Row {RowNumber} has {ValueCount} values but expected {HeaderCount}", 
                    i + 1, values.Length, headers.Length);
                continue;
            }

            var record = CreateDataRecord(headers, values, dataType, i);
            if (record != null)
            {
                records.Add(record);
            }
        }

        return records.ToArray();
    }

    private async Task<DataRecord[]> TransformJsonAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken)
    {
        var content = await File.ReadAllTextAsync(sourceFile.FilePath, cancellationToken);
        
        try
        {
            using var document = JsonDocument.Parse(content);
            var records = new List<DataRecord>();

            if (document.RootElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var element in document.RootElement.EnumerateArray())
                {
                    var record = CreateDataRecordFromJson(element, dataType);
                    if (record != null)
                    {
                        records.Add(record);
                    }
                }
            }
            else if (document.RootElement.ValueKind == JsonValueKind.Object)
            {
                var record = CreateDataRecordFromJson(document.RootElement, dataType);
                if (record != null)
                {
                    records.Add(record);
                }
            }

            return records.ToArray();
        }
        catch (JsonException ex)
        {
            _logger.LogError(ex, "Invalid JSON in file {FileName}", sourceFile.FileName);
            throw;
        }
    }

    private async Task<DataRecord[]> TransformExcelAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken)
    {
        // Excel transformation would require a library like EPPlus
        // For now, return empty array with a warning
        _logger.LogWarning("Excel transformation is not implemented yet for {FileName}", sourceFile.FileName);
        await Task.CompletedTask;
        return Array.Empty<DataRecord>();
    }

    private DataRecord? CreateDataRecord(string[] headers, string[] values, TnoDataType dataType, int rowIndex)
    {
        try
        {
            var data = new Dictionary<string, object>();
            var legal = new Dictionary<string, object>();
            var meta = new Dictionary<string, object>();

            // Map CSV columns to data fields
            for (int i = 0; i < headers.Length && i < values.Length; i++)
            {
                var header = headers[i].Trim();
                var value = values[i].Trim();

                if (!string.IsNullOrEmpty(value))
                {
                    data[header] = ParseValue(value);
                }
            }

            // Generate record ID
            var recordId = GenerateRecordId(dataType, data, rowIndex);

            // Set up legal and ACL
            legal["legaltags"] = new[] { _configuration.LegalTag };
            legal["otherRelevantDataCountries"] = new[] { "NL" };
            legal["status"] = "compliant";

            var acl = new Dictionary<string, object>
            {
                ["viewers"] = new[] { $"data.default.viewers@{_configuration.DataPartition}.dataservices.energy" },
                ["owners"] = new[] { $"data.default.owners@{_configuration.DataPartition}.dataservices.energy" }
            };

            // Set up metadata
            meta["kind"] = GetOsduKind(dataType);
            meta["source"] = "TNO";
            meta["createdBy"] = "OSDU-DataLoad-TNO";
            meta["createdDate"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

            return new DataRecord
            {
                Id = recordId,
                Kind = GetOsduKind(dataType),
                Data = data,
                Legal = legal,
                Acl = acl,
                //Meta = meta
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating data record from row {RowIndex}", rowIndex);
            return null;
        }
    }

    private DataRecord? CreateDataRecordFromJson(JsonElement element, TnoDataType dataType)
    {
        try
        {
            var data = new Dictionary<string, object>();
            var legal = new Dictionary<string, object>();
            var meta = new Dictionary<string, object>();

            // Extract data from JSON element
            foreach (var property in element.EnumerateObject())
            {
                data[property.Name] = ExtractJsonValue(property.Value);
            }

            // Generate record ID
            var recordId = GenerateRecordId(dataType, data, 0);

            // Set up legal and ACL
            legal["legaltags"] = new[] { _configuration.LegalTag };
            legal["otherRelevantDataCountries"] = new[] { "NL" };
            legal["status"] = "compliant";

            var acl = new Dictionary<string, object>
            {
                ["viewers"] = new[] { $"data.default.viewers@{_configuration.DataPartition}.dataservices.energy" },
                ["owners"] = new[] { $"data.default.owners@{_configuration.DataPartition}.dataservices.energy" }
            };

            // Set up metadata
            meta["kind"] = GetOsduKind(dataType);
            meta["source"] = "TNO";
            meta["createdBy"] = "OSDU-DataLoad-TNO";
            meta["createdDate"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

            return new DataRecord
            {
                Id = recordId,
                Kind = GetOsduKind(dataType),
                Data = data,
                Legal = legal,
                Acl = acl,
                //Meta = meta
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating data record from JSON element");
            return null;
        }
    }

    private object ExtractJsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString() ?? string.Empty,
            JsonValueKind.Number => element.TryGetInt32(out var intValue) ? intValue : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Array => element.EnumerateArray().Select(ExtractJsonValue).ToArray(),
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ExtractJsonValue(p.Value)),
            JsonValueKind.Null => string.Empty,
            _ => element.ToString()
        };
    }

    private string ParseValue(string value)
    {
        // Try to parse as different types and return as string
        if (int.TryParse(value, out var intValue))
            return intValue.ToString();

        if (double.TryParse(value, out var doubleValue))
            return doubleValue.ToString();

        if (bool.TryParse(value, out var boolValue))
            return boolValue.ToString().ToLowerInvariant();

        if (DateTime.TryParse(value, out var dateValue))
            return dateValue.ToString("yyyy-MM-ddTHH:mm:ssZ");

        return value;
    }

    private string GenerateRecordId(TnoDataType dataType, Dictionary<string, object> data, int rowIndex)
    {
        // Check if data already has an 'id' field (like in JSON input)
        if (data.TryGetValue("id", out var existingId) && !string.IsNullOrEmpty(existingId?.ToString()))
        {
            return ProcessExistingId(existingId.ToString()!);
        }

        // Generate ID based on data type following Python logic
        return dataType switch
        {
            TnoDataType.Wells => GenerateMasterDataId("Well", data),
            TnoDataType.Wellbores => GenerateMasterDataId("Wellbore", data),
            TnoDataType.ReferenceData => GenerateReferenceDataId(data),
            TnoDataType.WellboreTrajectories => GenerateWorkProductComponentId("WellboreTrajectory", data),
            TnoDataType.WellMarkers => GenerateWorkProductComponentId("WellMarker", data),
            TnoDataType.WellboreMarkers => GenerateWorkProductComponentId("WellboreMarker", data),
            TnoDataType.WellLogs => GenerateWorkProductComponentId("WellLog", data),
            TnoDataType.Horizons => GenerateWorkProductComponentId("SeismicHorizon", data),
            TnoDataType.Formations => GenerateReferenceDataId(data),
            TnoDataType.WellCompletions => GenerateWorkProductComponentId("WellCompletion", data),
            _ => GenerateGenericId(data)
        };
    }

    private string ProcessExistingId(string existingId)
    {
        // Replace {{NAMESPACE}} placeholder with actual data partition ID (like Python does)
        var processedId = existingId.Replace("{{NAMESPACE}}", _configuration.DataPartition);
        
        // Replace OSDU patterns with data partition patterns (like Python add_metadata function)
        var referencePattern = $"{_configuration.DataPartition}:reference-data";
        var masterPattern = $"{_configuration.DataPartition}:master-data";
        
        processedId = processedId
            .Replace("osdu:reference-data", referencePattern)
            .Replace("osdu:master-data", masterPattern);
            
        return processedId;
    }

    private string GenerateMasterDataId(string entityType, Dictionary<string, object> data)
    {
        // For master data, use data partition + master-data pattern
        var baseId = $"{_configuration.DataPartition}:master-data--{entityType}:";
        
        // Try to find a natural identifier
        var identifier = GetNaturalIdentifier(data) ?? Guid.NewGuid().ToString("N");
        
        return $"{baseId}{identifier}";
    }

    private string GenerateReferenceDataId(Dictionary<string, object> data)
    {
        // For reference data, use data partition + reference-data pattern
        var baseId = $"{_configuration.DataPartition}:reference-data--";
        
        // Try to determine the reference type
        var referenceType = GetReferenceType(data);
        var identifier = GetNaturalIdentifier(data) ?? Guid.NewGuid().ToString("N");
        
        return $"{baseId}{referenceType}:{identifier}";
    }

    private string GenerateWorkProductComponentId(string componentType, Dictionary<string, object> data)
    {
        // For work product components, use data partition + work-product-component pattern
        var baseId = $"{_configuration.DataPartition}:work-product-component--{componentType}:";
        
        var identifier = GetNaturalIdentifier(data) ?? Guid.NewGuid().ToString("N");
        
        return $"{baseId}{identifier}";
    }

    private string GenerateWorkProductId(string fileName, string baseDir)
    {
        // Matches Python generate_workproduct_id function
        return $"{_configuration.DataPartition}:work-product--WorkProduct:{baseDir}-{fileName}";
    }

    private string GenerateGenericId(Dictionary<string, object> data)
    {
        // Generic fallback
        var identifier = GetNaturalIdentifier(data) ?? Guid.NewGuid().ToString("N");
        return $"{_configuration.DataPartition}:dataset--File.Generic:{identifier}";
    }

    private string? GetNaturalIdentifier(Dictionary<string, object> data)
    {
        // Try to use natural identifiers from the data (similar to Python logic)
        var idCandidates = new[] { "id", "well_id", "wellbore_id", "name", "wellName", "wellboreName", "Name" };
        
        foreach (var candidate in idCandidates)
        {
            if (data.TryGetValue(candidate, out var value) && !string.IsNullOrEmpty(value?.ToString()))
            {
                // Clean the identifier (remove spaces, special chars, etc.)
                return CleanIdentifier(value.ToString()!);
            }
        }
        
        return null;
    }

    private string GetReferenceType(Dictionary<string, object> data)
    {
        // Try to infer reference type from data
        if (data.ContainsKey("formation") || data.ContainsKey("Formation"))
            return "GeologicFeature";
        if (data.ContainsKey("horizon") || data.ContainsKey("Horizon"))
            return "SeismicHorizon";
        if (data.ContainsKey("stratigraphy") || data.ContainsKey("Stratigraphy"))
            return "StratigraphicColumn";
            
        // Default reference type
        return "GenericReference";
    }

    private string CleanIdentifier(string identifier)
    {
        // Clean identifier similar to Python URL encoding behavior
        return Uri.EscapeDataString(identifier.Replace(" ", "_").Replace("/", "_"));
    }

    private string GetOsduKind(TnoDataType dataType)
    {
        return dataType switch
        {
            TnoDataType.Wells => "osdu:wks:master-data--Well:1.0.0",
            TnoDataType.Wellbores => "osdu:wks:master-data--Wellbore:1.0.0",
            TnoDataType.WellboreTrajectories => "osdu:wks:work-product-component--WellboreTrajectory:1.0.0",
            TnoDataType.WellMarkers => "osdu:wks:work-product-component--WellMarker:1.0.0",
            TnoDataType.WellboreMarkers => "osdu:wks:work-product-component--WellboreMarker:1.0.0",
            TnoDataType.WellLogs => "osdu:wks:work-product-component--WellLog:1.0.0",
            TnoDataType.ReferenceData => "osdu:wks:reference-data--StratigraphicColumn:1.0.0",
            TnoDataType.Horizons => "osdu:wks:work-product-component--SeismicHorizon:1.0.0",
            TnoDataType.Formations => "osdu:wks:reference-data--GeologicFeature:1.0.0",
            TnoDataType.WellCompletions => "osdu:wks:work-product-component--WellCompletion:1.0.0",
            _ => "osdu:wks:dataset--File.Generic:1.0.0"
        };
    }

    private void ValidateCsvContent(string content, List<string> errors, List<string> warnings)
    {
        var lines = content.Split('\n');
        
        if (lines.Length < 2)
        {
            errors.Add("CSV must have at least header and one data row");
            return;
        }

        var headerLine = lines[0].Trim();
        if (string.IsNullOrEmpty(headerLine))
        {
            errors.Add("CSV header row is empty");
            return;
        }

        var headers = headerLine.Split(',');
        if (headers.Length == 0)
        {
            errors.Add("No columns found in CSV header");
        }

        // Check for required columns based on data type
        // This is a simplified check - real implementation would be more comprehensive
        var requiredColumns = new[] { "id", "name" };
        var missingColumns = requiredColumns.Where(col => !headers.Any(h => h.Trim().Equals(col, StringComparison.OrdinalIgnoreCase)));
        
        foreach (var missing in missingColumns)
        {
            warnings.Add($"Recommended column '{missing}' not found");
        }
    }

    private void ValidateJsonContent(string content, List<string> errors, List<string> warnings)
    {
        try
        {
            using var document = JsonDocument.Parse(content);
            
            if (document.RootElement.ValueKind == JsonValueKind.Array)
            {
                if (document.RootElement.GetArrayLength() == 0)
                {
                    warnings.Add("JSON array is empty");
                }
            }
            else if (document.RootElement.ValueKind == JsonValueKind.Object)
            {
                // Valid single object
            }
            else
            {
                errors.Add("JSON root must be object or array");
            }
        }
        catch (JsonException ex)
        {
            errors.Add($"Invalid JSON: {ex.Message}");
        }
    }

    private async Task<List<DataRecord>> TransformLasAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Transforming LAS file: {FileName}", sourceFile.FileName);
        
        var records = new List<DataRecord>();
        
        // Basic LAS file parsing - this is a simplified implementation
        // In a real scenario, you'd use a specialized LAS parsing library
        var content = await File.ReadAllTextAsync(sourceFile.FilePath, cancellationToken);
        
        // Create data dictionary for the record
        var data = new Dictionary<string, object>
        {
            {"wellLogName", Path.GetFileNameWithoutExtension(sourceFile.FileName)},
            {"fileName", sourceFile.FileName},
            {"fileSize", sourceFile.Size},
            {"logType", "LAS"},
            {"description", $"Well log data from {sourceFile.FileName}"}
        };

        // Set up legal and ACL
        var legal = new Dictionary<string, object>
        {
            ["legaltags"] = new[] { _configuration.LegalTag },
            ["otherRelevantDataCountries"] = new[] { "NL" },
            ["status"] = "compliant"
        };

        var acl = new Dictionary<string, object>
        {
            ["viewers"] = new[] { $"data.default.viewers@{_configuration.DataPartition}.dataservices.energy" },
            ["owners"] = new[] { $"data.default.owners@{_configuration.DataPartition}.dataservices.energy" }
        };

        // Set up metadata
        var meta = new Dictionary<string, object>
        {
            ["kind"] = GetOsduKind(dataType),
            ["source"] = "TNO",
            ["createdBy"] = "OSDU-DataLoad-TNO",
            ["createdDate"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
        };
        
        // Create a basic well log record from the LAS file
        var record = new DataRecord
        {
            Id = GenerateRecordId(dataType, data, 0),
            Kind = GetOsduKind(dataType),
            Data = data,
            Legal = legal,
            Acl = acl,
            //Meta = meta,
            Tags = new Dictionary<string, string>
            {
                {"source", "TNO"},
                {"fileType", "las"}
            }
        };
        
        records.Add(record);
        _logger.LogInformation("Created well log record from LAS file: {FileName}", sourceFile.FileName);
        
        return records;
    }

    private async Task<List<DataRecord>> TransformDlisAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Transforming DLIS file: {FileName}", sourceFile.FileName);
        
        var records = new List<DataRecord>();
        
        // Create data dictionary for the record
        var data = new Dictionary<string, object>
        {
            {"wellLogName", Path.GetFileNameWithoutExtension(sourceFile.FileName)},
            {"fileName", sourceFile.FileName},
            {"fileSize", sourceFile.Size},
            {"logType", "DLIS"},
            {"description", $"Well log data from {sourceFile.FileName}"}
        };

        // Set up legal and ACL
        var legal = new Dictionary<string, object>
        {
            ["legaltags"] = new[] { _configuration.LegalTag },
            ["otherRelevantDataCountries"] = new[] { "NL" },
            ["status"] = "compliant"
        };

        var acl = new Dictionary<string, object>
        {
            ["viewers"] = new[] { $"data.default.viewers@{_configuration.DataPartition}.dataservices.energy" },
            ["owners"] = new[] { $"data.default.owners@{_configuration.DataPartition}.dataservices.energy" }
        };

        // Set up metadata
        var meta = new Dictionary<string, object>
        {
            ["kind"] = GetOsduKind(dataType),
            ["source"] = "TNO",
            ["createdBy"] = "OSDU-DataLoad-TNO",
            ["createdDate"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
        };
        
        // Basic DLIS file handling - this is a simplified implementation
        // In a real scenario, you'd use a specialized DLIS parsing library
        var record = new DataRecord
        {
            Id = GenerateRecordId(dataType, data, 0),
            Kind = GetOsduKind(dataType),
            Data = data,
            Legal = legal,
            Acl = acl,
            //Meta = meta,
            Tags = new Dictionary<string, string>
            {
                {"source", "TNO"},
                {"fileType", "dlis"}
            }
        };
        
        records.Add(record);
        _logger.LogInformation("Created well log record from DLIS file: {FileName}", sourceFile.FileName);
        
        await Task.CompletedTask; // Make it properly async
        return records;
    }

    private async Task<List<DataRecord>> TransformDocumentAsync(SourceFile sourceFile, TnoDataType dataType, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Transforming document file: {FileName}", sourceFile.FileName);
        
        var records = new List<DataRecord>();
        
        // Create data dictionary for the record
        var data = new Dictionary<string, object>
        {
            {"Name", Path.GetFileNameWithoutExtension(sourceFile.FileName)},
            {"fileName", sourceFile.FileName},
            {"fileSize", sourceFile.Size.ToString()},
            {"documentType", sourceFile.FileType.ToUpperInvariant()},
            {"description", $"Document from {sourceFile.FileName}"}
        };

        // Set up legal and ACL
        var legal = new Dictionary<string, object>
        {
            ["legaltags"] = new[] { _configuration.LegalTag },
            ["otherRelevantDataCountries"] = new[] { "NL" },
            ["status"] = "compliant"
        };

        var acl = new Dictionary<string, object>
        {
            ["viewers"] = new[] { $"data.default.viewers@{_configuration.DataPartition}.dataservices.energy" },
            ["owners"] = new[] { $"data.default.owners@{_configuration.DataPartition}.dataservices.energy" }
        };

        // Set up metadata
        var meta = new Dictionary<string, object>
        {
            ["kind"] = GetOsduKind(dataType),
            ["source"] = "TNO",
            ["createdBy"] = "OSDU-DataLoad-TNO",
            ["createdDate"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ")
        };
        
        // Basic document handling
        var record = new DataRecord
        {
            Id = GenerateRecordId(dataType, data, 0),
            Kind = GetOsduKind(dataType),
            Data = data,
            Legal = legal,
            Acl = acl,
            //Meta = meta,
            Tags = new Dictionary<string, string>
            {
                {"source", "TNO"},
                {"fileType", sourceFile.FileType.ToLowerInvariant()}
            }
        };
        
        records.Add(record);
        _logger.LogInformation("Created document record from file: {FileName}", sourceFile.FileName);
        
        await Task.CompletedTask; // Make it properly async
        return records;
    }
}
