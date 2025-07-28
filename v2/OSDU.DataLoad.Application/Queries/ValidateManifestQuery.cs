using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Queries;

/// <summary>
/// Query to validate a manifest
/// </summary>
public record ValidateManifestQuery : IRequest<ValidationResult>
{
    public LoadingManifest Manifest { get; init; } = null!;
}
