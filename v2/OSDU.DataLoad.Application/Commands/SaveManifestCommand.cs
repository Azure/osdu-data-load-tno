using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to save a manifest to file
/// </summary>
public record SaveManifestCommand : IRequest
{
    public LoadingManifest Manifest { get; init; } = null!;
    public string FilePath { get; init; } = string.Empty;
}
