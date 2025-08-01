using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for generating all TNO manifests (both work product and non-work product)
/// </summary>
public class GenerateManifestsCommandHandler : IRequestHandler<GenerateManifestsCommand, LoadResult>
{
    private readonly IMediator _mediator;
    private readonly ILogger<GenerateManifestsCommandHandler> _logger;

    public GenerateManifestsCommandHandler(IMediator mediator, ILogger<GenerateManifestsCommandHandler> logger)
    {
        _mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(GenerateManifestsCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        _logger.LogInformation("Starting manifest generation for all TNO data types");
        _logger.LogInformation("Source: {SourceDataPath}", request.SourceDataPath);
        _logger.LogInformation("Output: {OutputPath}", request.OutputPath);

        try
        {
            var overallSuccess = true;
            var totalProcessed = 0;
            var totalSuccessful = 0;
            var totalFailed = 0;
            var combinedMessages = new List<string>();

            var manifestDir = Path.Combine(request.OutputPath, "manifests");

            // Remove existing non-work product manifests directory
            if (Directory.Exists(manifestDir))
            {
                Directory.Delete(manifestDir, true);
            }


            // Step 1: Generate Non-Work Product Manifests (reference data, no dependencies)
            _logger.LogInformation("Generating non-work product manifests (reference data)");
            var nonWorkProductResult = await _mediator.Send(new GenerateNonWorkProductManifestCommand
            {
                SourceDataPath = request.SourceDataPath,
                OutputPath = manifestDir,
                DataPartition = request.DataPartition,
                LegalTag = request.LegalTag,
                AclViewer = request.AclViewer,
                AclOwner = request.AclOwner,
                ManifestConfigs = ManifestGenerationConfiguration.NonWorkProductManifestConfigs
            }, cancellationToken);

            overallSuccess = overallSuccess && nonWorkProductResult.IsSuccess;
            totalProcessed += nonWorkProductResult.ProcessedRecords;
            totalSuccessful += nonWorkProductResult.SuccessfulRecords;
            totalFailed += nonWorkProductResult.FailedRecords;

            if (!string.IsNullOrEmpty(nonWorkProductResult.Message))
                combinedMessages.Add($"Non-Work Product: {nonWorkProductResult.Message}");

            if (!nonWorkProductResult.IsSuccess)
                _logger.LogWarning("Non-work product manifest generation failed: {Error}", nonWorkProductResult.ErrorDetails);
            else
                _logger.LogInformation("Non-work product manifest generation completed successfully");

            // Step 2: Generate Work Product Manifests (depends on uploaded mapping files)
            _logger.LogInformation("Generating work product manifests (requires uploaded mapping files)");
            var workProductResult = await _mediator.Send(new GenerateWorkProductManifestCommand
            {
                SourceDataPath = request.SourceDataPath,
                WorkProductsMappingPath = Path.Combine(request.SourceDataPath, "output"),
                DataPartition = request.DataPartition,
                LegalTag = request.LegalTag,
                AclViewer = request.AclViewer,
                AclOwner = request.AclOwner,
                ManifestConfigs = ManifestGenerationConfiguration.WorkProductManifestConfigs
            }, cancellationToken);

            overallSuccess = overallSuccess && workProductResult.IsSuccess;
            totalProcessed += workProductResult.ProcessedRecords;
            totalSuccessful += workProductResult.SuccessfulRecords;
            totalFailed += workProductResult.FailedRecords;

            if (!string.IsNullOrEmpty(workProductResult.Message))
                combinedMessages.Add($"Work Product: {workProductResult.Message}");

            if (!workProductResult.IsSuccess)
                _logger.LogWarning("Work product manifest generation failed: {Error}", workProductResult.ErrorDetails);
            else
                _logger.LogInformation("Work product manifest generation completed successfully");

            // Combine results
            var duration = DateTime.UtcNow - startTime;
            var message = string.Join("; ", combinedMessages);

            var result = new LoadResult
            {
                IsSuccess = overallSuccess,
                ProcessedRecords = totalProcessed,
                SuccessfulRecords = totalSuccessful,
                FailedRecords = totalFailed,
                Duration = duration,
                Message = overallSuccess ? $"All manifests generated successfully. {message}" : $"Manifest generation completed with errors. {message}",
                ErrorDetails = overallSuccess ? string.Empty : "One or more manifest generation steps failed. Check logs for details."
            };

            _logger.LogInformation("Manifest generation completed - Success: {Success}, Processed: {Processed}, Duration: {Duration:mm\\:ss}",
                overallSuccess, totalProcessed, duration);

            return result;
        }
        catch (Exception ex)
        {
            var duration = DateTime.UtcNow - startTime;
            _logger.LogError(ex, "Error during manifest generation");

            return new LoadResult
            {
                IsSuccess = false,
                ProcessedRecords = 0,
                SuccessfulRecords = 0,
                FailedRecords = 0,
                Duration = duration,
                Message = "Manifest generation failed due to unexpected error",
                ErrorDetails = ex.Message
            };
        }
    }
}