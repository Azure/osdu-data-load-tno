using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to load TNO data into OSDU platform
/// </summary>
public record LoadDataCommand : IRequest<LoadResult>
{
    public string SourcePath { get; init; } = string.Empty;
    public TnoDataType DataType { get; init; }
}
