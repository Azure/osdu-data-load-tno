using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to load data from a manifest file
/// </summary>
public record LoadFromManifestCommand : IRequest<LoadResult>
{
    public LoadingManifest Manifest { get; init; } = null!;
}
