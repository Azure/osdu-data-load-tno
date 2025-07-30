using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to upload dataset files to OSDU and create location mapping files
/// This corresponds to the "LoadFiles" step in the Python solution
/// </summary>
public record UploadDatasetsCommand : IRequest<LoadResult>
{
    public string SourceDataPath { get; init; } = string.Empty;
    public string OutputPath { get; init; } = string.Empty;
}
