using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to upload records to OSDU
/// </summary>
public record UploadRecordsCommand : IRequest<LoadResult>
{
    public IEnumerable<DataRecord> Records { get; init; } = Enumerable.Empty<DataRecord>();
}
