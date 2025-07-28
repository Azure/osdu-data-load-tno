using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to transform source data to OSDU format
/// </summary>
public record TransformDataCommand : IRequest<DataRecord[]>
{
    public SourceFile SourceFile { get; init; } = null!;
    public TnoDataType DataType { get; init; }
}
