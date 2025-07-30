using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to load data from a manifest file or directory
/// </summary>
public record LoadFromManifestCommand : IRequest<LoadResult>
{
    public LoadingManifest? Manifest { get; init; }
    public string SourcePath { get; init; } = string.Empty;
    public TnoDataType DataType { get; init; }
    public string? FileLocationMapPath { get; init; }
    public bool IsWorkProduct { get; init; } = false;
}
