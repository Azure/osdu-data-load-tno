using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to generate a loading manifest
/// </summary>
public record GenerateManifestCommand : IRequest<LoadingManifest>
{
    public SourceFile[] SourceFiles { get; init; } = Array.Empty<SourceFile>();
    public TnoDataType DataType { get; init; }
}
