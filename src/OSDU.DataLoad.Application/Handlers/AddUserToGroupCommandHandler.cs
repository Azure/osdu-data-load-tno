using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Commands;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for adding a user to the OSDU data lake operations group
/// </summary>
public class AddUserToOpsGroupCommandHandler : IRequestHandler<AddUserToOpsGroupCommand, LoadResult>
{
    private readonly IOsduClient _osduClient;
    private readonly ILogger<AddUserToOpsGroupCommandHandler> _logger;

    public AddUserToOpsGroupCommandHandler(IOsduClient osduClient, ILogger<AddUserToOpsGroupCommandHandler> logger)
    {
        _osduClient = osduClient ?? throw new ArgumentNullException(nameof(osduClient));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<LoadResult> Handle(AddUserToOpsGroupCommand request, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        
        if (string.IsNullOrWhiteSpace(request.UserEmail))
        {
            _logger.LogInformation("No user email provided in command, skipping user addition");
            return new LoadResult
            {
                IsSuccess = true,
                Message = "No user email provided, skipped user addition",
                Duration = DateTime.UtcNow - startTime
            };
        }

        if (string.IsNullOrWhiteSpace(request.DataPartition))
        {
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Data partition is required",
                Duration = DateTime.UtcNow - startTime
            };
        }

        var groupName = $"users.datalake.ops@{request.DataPartition}.dataservices.energy";
        _logger.LogInformation("Adding user {UserEmail} to ops group", request.UserEmail);

        try
        {
            var success = await _osduClient.AddUserToOpsGroupAsync(request.DataPartition, request.UserEmail, cancellationToken);
            
            var result = new LoadResult
            {
                IsSuccess = success,
                Message = success ? "User added to ops group successfully" : "Failed to add user to ops group",
                Duration = DateTime.UtcNow - startTime
            };

            if (success)
            {
                _logger.LogInformation("Successfully added user {UserEmail} to ops group", request.UserEmail);
            }
            else
            {
                _logger.LogError("Failed to add user {UserEmail} to ops group", request.UserEmail);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error adding user {UserEmail} to ops group", request.UserEmail);
            return new LoadResult
            {
                IsSuccess = false,
                Message = "Error adding user to ops group",
                ErrorDetails = ex.Message,
                Duration = DateTime.UtcNow - startTime
            };
        }
    }
}
