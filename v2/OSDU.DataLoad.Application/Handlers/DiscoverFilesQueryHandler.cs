using MediatR;
using Microsoft.Extensions.Logging;
using OSDU.DataLoad.Application.Queries;
using OSDU.DataLoad.Domain.Entities;
using OSDU.DataLoad.Domain.Interfaces;

namespace OSDU.DataLoad.Application.Handlers;

/// <summary>
/// Handler for discovering files
/// </summary>
public class DiscoverFilesQueryHandler : IRequestHandler<DiscoverFilesQuery, SourceFile[]>
{
    private readonly IFileProcessor _fileProcessor;
    private readonly ILogger<DiscoverFilesQueryHandler> _logger;

    public DiscoverFilesQueryHandler(IFileProcessor fileProcessor, ILogger<DiscoverFilesQueryHandler> logger)
    {
        _fileProcessor = fileProcessor ?? throw new ArgumentNullException(nameof(fileProcessor));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<SourceFile[]> Handle(DiscoverFilesQuery request, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Discovering {DataType} files in {DirectoryPath}", 
            request.DataType, request.DirectoryPath);

        return await _fileProcessor.DiscoverFilesAsync(request.DirectoryPath, request.DataType, cancellationToken);
    }
}
