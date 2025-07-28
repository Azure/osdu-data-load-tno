using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Queries;

/// <summary>
/// Query to discover source files in a directory
/// </summary>
public record DiscoverFilesQuery : IRequest<SourceFile[]>
{
    public string DirectoryPath { get; init; } = string.Empty;
    public TnoDataType DataType { get; init; }
}
