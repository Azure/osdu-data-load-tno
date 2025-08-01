using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to generate work product manifests
/// </summary>
public record GenerateWorkProductManifestCommand : IRequest<LoadResult>
{
    public string SourceDataPath { get; init; } = string.Empty;
    public string WorkProductsMappingPath { get; init; } = string.Empty;
    public string DataPartition { get; init; } = string.Empty;
    public string LegalTag { get; init; } = string.Empty;
    public string AclViewer { get; init; } = string.Empty;
    public string AclOwner { get; init; } = string.Empty;

    public IEnumerable<ManifestGenerationConfig> ManifestConfigs { get; init; } = Enumerable.Empty<ManifestGenerationConfig>();

}
