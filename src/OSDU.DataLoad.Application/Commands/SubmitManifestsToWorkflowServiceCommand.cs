using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to submit manifest files to OSDU Workflow Service
/// </summary>
public record SubmitManifestsToWorkflowServiceCommand : IRequest<LoadResult>
{
    public string SourceDataPath { get; init; } = string.Empty;
    public string DataPartition { get; init; } = string.Empty;
}
