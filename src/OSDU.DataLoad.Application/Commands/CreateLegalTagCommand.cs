using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to create a legal tag in OSDU
/// </summary>
public record CreateLegalTagCommand : IRequest<LoadResult>
{
    public string LegalTagName { get; init; } = string.Empty;
}
