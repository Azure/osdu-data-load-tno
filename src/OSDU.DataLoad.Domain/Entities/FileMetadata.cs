namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// File metadata for OSDU file upload
/// </summary>
public class FileMetadata
{
    public string Kind { get; set; } = "osdu:wks:dataset--File.Generic:1.0.0";
    public Dictionary<string, object> Acl { get; set; } = new();
    public Dictionary<string, object> Legal { get; set; } = new();
    public Dictionary<string, object> Data { get; set; } = new();
}

/// <summary>
/// Response from file metadata creation
/// </summary>
public class FileMetadataResponse
{
    public string? Id { get; set; }
}

/// <summary>
/// Response from storage versions API
/// </summary>
public class StorageVersionsResponse
{
    public long? Version { get; set; }
}
