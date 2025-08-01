using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to upload files from configured dataset directories to OSDU
/// </summary>
public class UploadFilesCommand : IRequest<LoadResult>
{
    public string BasePath { get; init; } = string.Empty;
    public string OutputPath { get; init; } = string.Empty;

    public UploadFilesCommand(string basePath, string outputPath)
    {
        BasePath = basePath ?? throw new ArgumentNullException(nameof(basePath));
        OutputPath = outputPath ?? throw new ArgumentNullException(nameof(outputPath));
    }
}
