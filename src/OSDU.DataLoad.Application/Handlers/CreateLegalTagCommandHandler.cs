using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for creating a legal tag in OSDU
/// </summary>
public class CreateLegalTagCommandHandler : IRequestHandler<CreateLegalTagCommand, LoadResult>
{
    private readonly IOsduClient _osduClient;
    private readonly ILogger<CreateLegalTagCommandHandler> _logger;

    public CreateLegalTagCommandHandler(IOsduClient osduClient, ILogger<CreateLegalTagCommandHandler> logger)
    {
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(CreateLegalTagCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        
        if (string.IsNullOrWhiteSpace(request.LegalTagName))
        {
            _logger.LogInformation("No legal tag name provided in command, skipping legal tag creation");
            return new LoadResult
            {
                IsSuccess = true,
                Message = "No legal tag name provided, skipped legal tag creation",
                Duration = DateTime.UtcNow - startTime
            };
        }

        _logger.LogInformation("Creating legal tag {LegalTagName}", request.LegalTagName);

        try
        {
            var success = await _osduClient.CreateLegalTagAsync(request.LegalTagName, cancellationToken);
            
            var result = new LoadResult
            {
                IsSuccess = success,
                Message = success ? "Legal tag created successfully" : "Failed to create legal tag",
                Duration = DateTime.UtcNow - startTime
            };

            if (success)
            {
                _logger.LogInformation("Successfully created legal tag {LegalTagName}", request.LegalTagName);
            }
            else
            {
                _logger.LogError("Failed to create legal tag {LegalTagName}", request.LegalTagName);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating legal tag {LegalTagName}", request.LegalTagName);
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Error creating legal tag",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }
}
