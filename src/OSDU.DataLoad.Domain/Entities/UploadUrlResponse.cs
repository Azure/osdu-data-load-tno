using System.Text.Json.Serialization;

namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Response from OSDU upload URL request
/// </summary>
public class UploadUrlResponse
{
    [JsonPropertyName("Location")]
    public UploadLocation? Location { get; set; }
    
    [JsonPropertyName("FileID")]
    public string? FileID { get; set; }
}

/// <summary>
/// Location information for file upload
/// </summary>
public class UploadLocation
{
    [JsonPropertyName("SignedURL")]
    public string? SignedURL { get; set; }
    
    [JsonPropertyName("FileSource")]
    public string? FileSource { get; set; }
}
