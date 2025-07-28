using MediatR;

namespace OSDU.DataLoad.Application.Queries;

/// <summary>
/// Query to check if a record exists in OSDU
/// </summary>
public record RecordExistsQuery : IRequest<bool>
{
    public string RecordId { get; init; } = string.Empty;
}
