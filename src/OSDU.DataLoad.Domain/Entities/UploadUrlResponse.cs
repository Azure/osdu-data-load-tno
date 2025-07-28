namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Response from OSDU upload URL request
/// </summary>
public class UploadUrlResponse
{
    public UploadLocation? Location { get; set; }
    public string? FileID { get; set; }
}

/// <summary>
/// Location information for file upload
/// </summary>
public class UploadLocation
{
    public string? SignedURL { get; set; }
    public string? FileSource { get; set; }
}
