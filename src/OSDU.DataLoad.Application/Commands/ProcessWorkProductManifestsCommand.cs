using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to process work product manifest templates and update them with file location references
/// This step happens between file upload and work product loading, similar to Python's approach
/// </summary>
public record ProcessWorkProductManifestsCommand : IRequest<LoadResult>
{
    public string SourceDataPath { get; init; } = string.Empty;
    public string OutputPath { get; init; } = string.Empty;
    public Dictionary<TnoDataType, string> FileLocationMappings { get; init; } = new();
}
