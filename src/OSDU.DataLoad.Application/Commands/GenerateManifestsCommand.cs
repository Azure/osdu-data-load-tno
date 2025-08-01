using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to generate all TNO manifests from CSV data using templates
/// </summary>
public record GenerateManifestsCommand : IRequest<LoadResult>
{
    public string SourceDataPath { get; init; } = string.Empty;
    public string OutputPath { get; init; } = string.Empty;
    public string DataPartition { get; init; } = string.Empty;

    public string AclViewer { get; init; } = string.Empty;

    public string AclOwner { get; init; } = string.Empty;

    public string LegalTag { get; init; } = string.Empty;
}
