using MediatR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for loading all TNO data types in the correct order
/// </summary>
public class LoadAllDataCommandHandler : IRequestHandler<LoadAllDataCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<LoadAllDataCommandHandler> _logger;
    private readonly OsduConfiguration _configuration;

    public LoadAllDataCommandHandler(IMediator mediator, ILogger<LoadAllDataCommandHandler> logger, IOptions<OsduConfiguration> configuration)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _configuration = configuration?.Value ?? throw new ArgumentNullException(nameof(configuration));
    }

    public async Task<LoadResult> Handle(LoadAllDataCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting complete TNO data load operation from {SourcePath}", request.SourcePath);

        if (string.IsNullOrWhiteSpace(request.SourcePath))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Source path is required",
                Duration = DateTime.UtcNow - startTime
            };
        }

        if (!Directory.Exists(request.SourcePath))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = $"Source directory does not exist: {request.SourcePath}",
                Duration = DateTime.UtcNow - startTime
            };
        }

        var overallResult = new LoadResult
        {
            IsSuccess = true,
            ProcessedRecords = 0,
            SuccessfulRecords = 0,
            FailedRecords = 0
        };

        var phaseResults = new List<(TnoDataType DataType, LoadResult Result)>();

        try
        {
            // Prepare stage: Add user to authorization group if specified
            _logger.LogInformation("Starting prepare stage - checking for user authorization setup");
            
            if (!string.IsNullOrWhiteSpace(_configuration.UserEmail))
            {
                var addUserResult = await _mediator.Send(new AddUserToOpsGroupCommand
                {
                    DataPartition = _configuration.DataPartition,
                    UserEmail = _configuration.UserEmail
                }, cancellationToken);

                if (!addUserResult.IsSuccess)
                {
                    _logger.LogWarning("Failed to add user to authorization group, but continuing with data load: {Message}", addUserResult.Message);
                }
            }
            else
            {
                _logger.LogInformation("No user email configured, skipping user authorization setup");
            }

            // Create legal tag if specified
            _logger.LogInformation("Creating legal tag");
            var createLegalTagResult = await _mediator.Send(new CreateLegalTagCommand
            {
                LegalTagName = _configuration.LegalTag
            }, cancellationToken);

            if (!createLegalTagResult.IsSuccess)
            {
                _logger.LogWarning("Failed to create legal tag, message: {Message}", createLegalTagResult.Message);
                return new LoadResult
                {
                    IsSuccess = false,
                    Message = $"Failed to create legal tag {createLegalTagResult.Message}",
                    Duration = DateTime.UtcNow - startTime
                };
            }

            // Load data types in the correct order
            foreach (var dataType in DataLoadingOrder.LoadingSequence)
            {
                _logger.LogInformation("Starting load phase for {DataType}", dataType);

                // Check if the subdirectory exists for this data type
                var subdirectory = DataLoadingOrder.DirectoryMapping[dataType];
                var dataTypePath = Path.Combine(request.SourcePath, subdirectory);

                if (!Directory.Exists(dataTypePath))
                {
                    _logger.LogWarning("Skipping {DataType} - directory not found: {Path}", dataType, dataTypePath);
                    continue;
                }

                // Load data for this type
                var phaseStartTime = DateTime.UtcNow;
                _logger.LogInformation("Loading {DataType} from {Path}", dataType, dataTypePath);

                try
                {
                    var result = await _mediator.Send(new LoadDataCommand
                    {
                        SourcePath = dataTypePath,
                        DataType = dataType
                    }, cancellationToken);

                    phaseResults.Add((dataType, result));

                    // Aggregate results
                    overallResult = new LoadResult
                    {
                        IsSuccess = overallResult.IsSuccess && result.IsSuccess,
                        ProcessedRecords = overallResult.ProcessedRecords + result.ProcessedRecords,
                        SuccessfulRecords = overallResult.SuccessfulRecords + result.SuccessfulRecords,
                        FailedRecords = overallResult.FailedRecords + result.FailedRecords,
                        Duration = DateTime.UtcNow - startTime
                    };

                    var phaseTime = DateTime.UtcNow - phaseStartTime;
                    _logger.LogInformation("Completed {DataType} in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} records successful",
                        dataType, phaseTime, result.SuccessfulRecords, result.ProcessedRecords);

                    if (!result.IsSuccess)
                    {
                        _logger.LogWarning("Phase {DataType} completed with errors: {Message}", dataType, result.Message);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to load {DataType} from {Path}", dataType, dataTypePath);
                    
                    var failedResult = new LoadResult
                    {
                        IsSuccess = false,
                        Message = $"Error loading {dataType}: {ex.Message}",
                        Duration = DateTime.UtcNow - phaseStartTime
                    };
                    
                    phaseResults.Add((dataType, failedResult));
                    overallResult = new LoadResult
                    {
                        IsSuccess = false,
                        ProcessedRecords = overallResult.ProcessedRecords,
                        SuccessfulRecords = overallResult.SuccessfulRecords,
                        FailedRecords = overallResult.FailedRecords,
                        Duration = overallResult.Duration,
                        Message = overallResult.Message,
                        ErrorDetails = overallResult.ErrorDetails
                    };
                }
            }

            // Generate summary message
            var summary = GenerateSummaryMessage(phaseResults, overallResult);
            overallResult = new LoadResult
            {
                IsSuccess = overallResult.IsSuccess,
                ProcessedRecords = overallResult.ProcessedRecords,
                SuccessfulRecords = overallResult.SuccessfulRecords,
                FailedRecords = overallResult.FailedRecords,
                Message = summary,
                Duration = DateTime.UtcNow - startTime,
                ErrorDetails = overallResult.ErrorDetails
            };

            _logger.LogInformation("Completed TNO data load operation in {Duration:mm\\:ss} - {SuccessfulRecords}/{ProcessedRecords} total records successful",
                overallResult.Duration, overallResult.SuccessfulRecords, overallResult.ProcessedRecords);

            return overallResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Critical error during TNO data load operation");
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Critical error during data load operation",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }

    private string GenerateSummaryMessage(List<(TnoDataType DataType, LoadResult Result)> phaseResults, LoadResult overallResult)
    {
        var summary = new List<string>
        {
            $"TNO Data Load Complete - {(overallResult.IsSuccess ? "SUCCESS" : "COMPLETED WITH ERRORS")}",
            "",
            "Phase Results:"
        };

        foreach (var (dataType, result) in phaseResults)
        {
            var status = result.IsSuccess ? "✅" : "❌";
            var rate = result.ProcessedRecords > 0 ? (double)result.SuccessfulRecords / result.ProcessedRecords * 100 : 0;
            summary.Add($"  {status} {dataType}: {result.SuccessfulRecords}/{result.ProcessedRecords} records ({rate:F1}%) in {result.Duration:mm\\:ss}");
        }

        summary.Add("");
        summary.Add($"Total: {overallResult.SuccessfulRecords}/{overallResult.ProcessedRecords} records in {overallResult.Duration:mm\\:ss}");

        if (overallResult.ProcessedRecords > 0)
        {
            var overallRate = (double)overallResult.SuccessfulRecords / overallResult.ProcessedRecords * 100;
            summary.Add($"Overall Success Rate: {overallRate:F1}%");
        }

        return string.Join(Environment.NewLine, summary);
    }
}
