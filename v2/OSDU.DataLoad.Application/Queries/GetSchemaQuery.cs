using MediatR;

namespace OSDU.DataLoad.Application.Queries;

/// <summary>
/// Query to get schema information for a specific kind
/// </summary>
public record GetSchemaQuery : IRequest<string>
{
    public string Kind { get; init; } = string.Empty;
}
