using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to download and setup TNO test data from the official repository
/// </summary>
public record DownloadDataCommand : IRequest<LoadResult>
{
    public string DestinationPath { get; init; } = string.Empty;
    public bool OverwriteExisting { get; init; } = false;
}
