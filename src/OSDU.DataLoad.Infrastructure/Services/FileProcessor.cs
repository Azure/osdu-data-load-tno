using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;
using Microsoft.Extensions.Logging;

namespace OSDU.DataLoad.Infrastructure.Services;

/// <summary>
/// File processor for discovering and reading source files
/// </summary>
public class FileProcessor : IFileProcessor
{
    private readonly ILogger<FileProcessor> _logger;

    public FileProcessor(ILogger<FileProcessor> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<SourceFile[]> DiscoverFilesAsync(string directoryPath, TnoDataType dataType, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Discovering files in {DirectoryPath} for data type {DataType}", directoryPath, dataType);

        if (!Directory.Exists(directoryPath))
        {
            _logger.LogWarning("Directory does not exist: {DirectoryPath}", directoryPath);
            return Array.Empty<SourceFile>();
        }

        await Task.CompletedTask; // Placeholder for async operations

        var supportedExtensions = GetSupportedExtensions(dataType);
        var sourceFiles = new List<SourceFile>();

        foreach (var extension in supportedExtensions)
        {
            var searchPattern = $"*.{extension}";
            var files = Directory.GetFiles(directoryPath, searchPattern, SearchOption.AllDirectories);

            foreach (var filePath in files)
            {
                try
                {
                    var fileInfo = new FileInfo(filePath);
                    var sourceFile = new SourceFile
                    {
                        FilePath = filePath,
                        FileName = fileInfo.Name,
                        FileType = extension,
                        Size = fileInfo.Length,
                        LastModified = fileInfo.LastWriteTimeUtc
                    };

                    sourceFiles.Add(sourceFile);
                    _logger.LogDebug("Found file: {FileName} ({Size} bytes)", sourceFile.FileName, sourceFile.Size);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error processing file: {FilePath}", filePath);
                }
            }
        }

        _logger.LogInformation("Discovered {FileCount} files for data type {DataType}", sourceFiles.Count, dataType);
        return sourceFiles.ToArray();
    }

    public async Task<T> ReadFileAsync<T>(SourceFile sourceFile, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Reading file: {FilePath}", sourceFile.FilePath);

        if (!File.Exists(sourceFile.FilePath))
        {
            throw new FileNotFoundException($"Source file not found: {sourceFile.FilePath}");
        }

        try
        {
            var content = await File.ReadAllTextAsync(sourceFile.FilePath, cancellationToken);
            
            // For now, return the content as-is
            // In a real implementation, this would parse based on file type
            if (typeof(T) == typeof(string))
            {
                return (T)(object)content;
            }

            throw new NotSupportedException($"File type {typeof(T)} is not supported");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading file: {FilePath}", sourceFile.FilePath);
            throw;
        }
    }

    public async Task<ValidationResult> ValidateFileAsync(SourceFile sourceFile, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Validating file: {FilePath}", sourceFile.FilePath);

        var errors = new List<string>();
        var warnings = new List<string>();

        await Task.CompletedTask; // Placeholder for async operations

        // Check if file exists
        if (!File.Exists(sourceFile.FilePath))
        {
            errors.Add($"File does not exist: {sourceFile.FilePath}");
        }
        else
        {
            try
            {
                var fileInfo = new FileInfo(sourceFile.FilePath);

                // Check file size
                if (fileInfo.Length == 0)
                {
                    errors.Add("File is empty");
                }
                else if (fileInfo.Length > 100 * 1024 * 1024) // 100MB
                {
                    warnings.Add($"Large file detected ({fileInfo.Length / (1024 * 1024)} MB)");
                }

                // Check file extension
                var extension = Path.GetExtension(sourceFile.FilePath).TrimStart('.');
                if (string.IsNullOrEmpty(extension))
                {
                    warnings.Add("File has no extension");
                }

                // Check if file is readable
                try
                {
                    using var stream = File.OpenRead(sourceFile.FilePath);
                    var buffer = new byte[1024];
                    await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                }
                catch (UnauthorizedAccessException)
                {
                    errors.Add("File is not readable (access denied)");
                }
                catch (IOException ex)
                {
                    errors.Add($"File I/O error: {ex.Message}");
                }

                // Validate file format based on extension
                switch (extension.ToLowerInvariant())
                {
                    case "csv":
                        await ValidateCsvFileAsync(sourceFile, errors, warnings, cancellationToken);
                        break;
                    case "json":
                        await ValidateJsonFileAsync(sourceFile, errors, warnings, cancellationToken);
                        break;
                    case "xlsx":
                    case "xls":
                        ValidateExcelFile(sourceFile, errors, warnings);
                        break;
                    default:
                        warnings.Add($"Unknown file format: {extension}");
                        break;
                }
            }
            catch (Exception ex)
            {
                errors.Add($"Error validating file: {ex.Message}");
            }
        }

        var isValid = errors.Count == 0;

        _logger.LogInformation("File validation completed for {FilePath}. Valid: {IsValid}, Errors: {ErrorCount}, Warnings: {WarningCount}",
            sourceFile.FilePath, isValid, errors.Count, warnings.Count);

        return new ValidationResult
        {
            IsValid = isValid,
            Errors = errors.ToArray(),
            Warnings = warnings.ToArray()
        };
    }

    private async Task ValidateCsvFileAsync(SourceFile sourceFile, List<string> errors, List<string> warnings, CancellationToken cancellationToken)
    {
        try
        {
            var lines = await File.ReadAllLinesAsync(sourceFile.FilePath, cancellationToken);
            
            if (lines.Length == 0)
            {
                errors.Add("CSV file is empty");
                return;
            }

            if (lines.Length == 1)
            {
                warnings.Add("CSV file contains only header row");
                return;
            }

            // Check header row
            var headerLine = lines[0];
            if (string.IsNullOrWhiteSpace(headerLine))
            {
                errors.Add("CSV header row is empty");
                return;
            }

            var headers = headerLine.Split(',');
            if (headers.Length == 0)
            {
                errors.Add("No columns found in CSV header");
            }

            // Check for empty column names
            for (int i = 0; i < headers.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(headers[i]))
                {
                    warnings.Add($"Empty column name at position {i + 1}");
                }
            }

            // Sample a few data rows to check structure
            var sampleSize = Math.Min(10, lines.Length - 1);
            for (int i = 1; i <= sampleSize; i++)
            {
                var dataLine = lines[i];
                if (string.IsNullOrWhiteSpace(dataLine))
                {
                    warnings.Add($"Empty data row at line {i + 1}");
                    continue;
                }

                var columns = dataLine.Split(',');
                if (columns.Length != headers.Length)
                {
                    warnings.Add($"Column count mismatch at line {i + 1}: expected {headers.Length}, got {columns.Length}");
                }
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Error validating CSV file: {ex.Message}");
        }
    }

    private async Task ValidateJsonFileAsync(SourceFile sourceFile, List<string> errors, List<string> warnings, CancellationToken cancellationToken)
    {
        try
        {
            var content = await File.ReadAllTextAsync(sourceFile.FilePath, cancellationToken);
            
            if (string.IsNullOrWhiteSpace(content))
            {
                errors.Add("JSON file is empty");
                return;
            }

            // Try to parse JSON
            using var document = System.Text.Json.JsonDocument.Parse(content);
            
            if (document.RootElement.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                var arrayLength = document.RootElement.GetArrayLength();
                if (arrayLength == 0)
                {
                    warnings.Add("JSON array is empty");
                }
                else if (arrayLength > 10000)
                {
                    warnings.Add($"Large JSON array detected ({arrayLength} items)");
                }
            }
            else if (document.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                // Valid JSON object
            }
            else
            {
                warnings.Add("JSON root element is neither object nor array");
            }
        }
        catch (System.Text.Json.JsonException ex)
        {
            errors.Add($"Invalid JSON format: {ex.Message}");
        }
        catch (Exception ex)
        {
            errors.Add($"Error validating JSON file: {ex.Message}");
        }
    }

    private void ValidateExcelFile(SourceFile sourceFile, List<string> errors, List<string> warnings)
    {
        // For now, just check that it's an Excel file
        // In a real implementation, you would use a library like EPPlus to validate Excel files
        var extension = Path.GetExtension(sourceFile.FilePath).ToLowerInvariant();
        if (extension != ".xlsx" && extension != ".xls")
        {
            errors.Add("Not a valid Excel file extension");
        }

        warnings.Add("Excel file validation is not fully implemented");
    }

    private string[] GetSupportedExtensions(TnoDataType dataType)
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
            TnoDataType.Documents => new[] { "pdf", "doc", "docx", "txt", "csv", "json" },
            TnoDataType.MiscMasterData => new[] { "csv", "json", "xlsx" },
            TnoDataType.WorkProducts => new[] { "csv", "json", "xlsx", "las", "pdf", "txt" },
            _ => new[] { "csv", "json" }
        };
    }
}
