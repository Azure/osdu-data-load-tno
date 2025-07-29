using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to add a user to the OSDU data lake operations group
/// </summary>
public record AddUserToOpsGroupCommand : IRequest<LoadResult>
{
    public string DataPartition { get; init; } = string.Empty;
    public string UserEmail { get; init; } = string.Empty;
}
