using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to load all TNO data types from a source directory in the correct order
/// </summary>
public record LoadAllDataCommand : IRequest<LoadResult>
{
    public string SourcePath { get; init; } = string.Empty;
}
