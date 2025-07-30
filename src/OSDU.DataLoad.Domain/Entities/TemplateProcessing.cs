using System.Text.Json.Serialization;

namespace OSDU.DataLoad.Domain.Entities;

/// <summary>
/// Represents a template parameter found in JSON templates
/// </summary>
public class TemplateParameter
{
    public string Parameter { get; set; } = string.Empty;
    public List<ParameterLocation> Locations { get; set; } = new();
}

/// <summary>
/// Represents the location of a parameter in the template structure
/// </summary>
public class ParameterLocation
{
    public object RootObject { get; set; } = new();
    public List<object> Keys { get; set; } = new();
    public List<RootListItem> RootList { get; set; } = new();
}

/// <summary>
/// Represents an item in the root list for nested array processing
/// </summary>
public class RootListItem
{
    public object RootObject { get; set; } = new();
    public List<object> Keys { get; set; } = new();
}

/// <summary>
/// Represents the mapping of CSV columns to template parameters
/// </summary>
public class ParameterColumnMapping
{
    public Dictionary<string, List<List<int>>> ParameterToColumns { get; set; } = new();
}

/// <summary>
/// Represents special template processing options
/// </summary>
public class TemplateProcessingOptions
{
    public string? SchemaPath { get; set; }
    public string? SchemaNamespaceName { get; set; }
    public string? SchemaNamespaceValue { get; set; }
    public string? ArrayParent { get; set; }
    public string? ObjectParent { get; set; }
    public string? GroupFilename { get; set; }
    public bool ValidateSchema { get; set; } = false;
}

/// <summary>
/// Constants for special template tags
/// </summary>
public static class TemplateConstants
{
    public const string ParameterStartDelimiter = "{{";
    public const string ParameterEndDelimiter = "}}";
    public const string TagFileName = "$filename";
    public const string TagSchemaId = "$schema";
    public const string TagArrayParent = "$array_parent";
    public const string TagObjectParent = "$object_parent";
    public const string TagKindParent = "$kind_parent";
    public const string TagRequiredTemplate = "$required_template";
    public const string TagOptionalField = "$";
    
    // Regex patterns for special tags
    public const string PatternOneOf = @"\$\$_oneOf_[1-9][0-9]*\$\$";
    public const string PatternAnyOf = @"\$\$_anyOf_[1-9][0-9]*\$\$";
}

/// <summary>
/// Represents the result of processing a single CSV row
/// </summary>
public class ManifestProcessingResult
{
    public bool IsSuccess { get; set; }
    public object? GeneratedManifest { get; set; }
    public string? OutputFileName { get; set; }
    public string? ErrorMessage { get; set; }
    public int RowNumber { get; set; }
    public List<string>? GeneratedFiles { get; set; }
}
