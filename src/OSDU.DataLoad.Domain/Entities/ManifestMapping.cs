using System.Text.Json;
using System.Text.Json.Serialization;

namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Represents the mapping configuration for generating manifests from CSV data
/// </summary>
public class ManifestMappingConfig
{
    [JsonPropertyName("required_template")]
    public JsonElement? RequiredTemplateElement { get; set; } = null;

    // Helper property to get the JSON as a string, defaulting to null
    [JsonIgnore]
    public string? RequiredTemplate => RequiredTemplateElement?.GetRawText();

    [JsonPropertyName("mapping")]
    public ManifestMapping[] Mapping { get; set; } = Array.Empty<ManifestMapping>();
}

/// <summary>
/// Represents a single mapping entry from CSV to manifest
/// </summary>
public class ManifestMapping
{
    [JsonPropertyName("data_file")]
    public string DataFile { get; set; } = string.Empty;

    [JsonPropertyName("template_file")]
    public string TemplateFile { get; set; } = string.Empty;

    [JsonPropertyName("output_file_name")]
    public string OutputFileName { get; set; } = string.Empty;
}

/// <summary>
/// ACL template for OSDU records
/// </summary>
public class AclTemplate
{
    [JsonPropertyName("owners")]
    public string[] Owners { get; set; } = Array.Empty<string>();

    [JsonPropertyName("viewers")]
    public string[] Viewers { get; set; } = Array.Empty<string>();
}

/// <summary>
/// Legal template for OSDU records
/// </summary>
public class LegalTemplate
{
    [JsonPropertyName("legaltags")]
    public string[] LegalTags { get; set; } = Array.Empty<string>();

    [JsonPropertyName("otherRelevantDataCountries")]
    public string[] OtherRelevantDataCountries { get; set; } = Array.Empty<string>();
}
