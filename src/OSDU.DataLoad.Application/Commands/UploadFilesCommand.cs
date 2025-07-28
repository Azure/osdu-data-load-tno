using MediatR;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Application.Commands;

/// <summary>
/// Command to upload files to OSDU
/// </summary>
public class UploadFilesCommand : IRequest<LoadResult>
{
    public IEnumerable<SourceFile> Files { get; init; } = Enumerable.Empty<SourceFile>();
    public string OutputPath { get; init; } = string.Empty;

    public UploadFilesCommand(IEnumerable<SourceFile> files, string outputPath)
    {
        Files = files ?? throw new ArgumentNullException(nameof(files));
        OutputPath = outputPath ?? throw new ArgumentNullException(nameof(outputPath));
    }
}
