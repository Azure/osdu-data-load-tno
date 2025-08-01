namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Result of file upload operation
/// </summary>
public class FileUploadResult
{
    public bool IsSuccess { get; set; }
    public string? Message { get; set; }
    public string? ErrorDetails { get; set; }
    public string? FileId { get; set; }
    public string? FileSource { get; set; }
    public string? FileRecordVersion { get; set; }
    public string? Description { get; set; }
}
