using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Queries;

/// <summary>
/// Query to validate source files
/// </summary>
public record ValidateSourceQuery : IRequest<ValidationResult>
{
    public SourceFile SourceFile { get; init; } = null!;
}
