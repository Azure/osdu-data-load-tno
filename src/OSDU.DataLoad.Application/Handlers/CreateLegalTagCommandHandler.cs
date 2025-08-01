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
    private readonly IOsduService _osduService;
    private readonly ILogger<CreateLegalTagCommandHandler> _logger;

    public CreateLegalTagCommandHandler(IOsduService osduService, ILogger<CreateLegalTagCommandHandler> logger)
    {
        _osduService = osduService ?? throw new ArgumentNullException(nameof(osduService));
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
            var result = await _osduService.CreateLegalTagAsync(request.LegalTagName, cancellationToken);
            
            _logger.LogInformation("Legal tag creation completed - Success: {IsSuccess}", result.IsSuccess);
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
