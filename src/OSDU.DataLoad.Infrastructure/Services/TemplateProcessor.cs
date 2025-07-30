using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using OSDU.DataLoad.Domain.Entities;

namespace OSDU.DataLoad.Infrastructure.Services;

/// <summary>
/// Advanced template processor that mirrors the Python csv_to_json logic
/// </summary>
public class TemplateProcessor
{
    private readonly JsonSerializerOptions _jsonOptions;

    public TemplateProcessor()
    {
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = true,
            PropertyNameCaseInsensitive = true
        };
    }

    /// <summary>
    /// Extract all template parameters from a JSON template
    /// Equivalent to Python's extract_template_parameters_from_json
    /// </summary>
    public TemplateParameter[] ExtractTemplateParameters(object templateObject)
    {
        var parameters = new Dictionary<string, TemplateParameter>();
        ExtractParametersRecursive(templateObject, parameters, new List<object>());
        
        return parameters.Values.ToArray();
    }

    /// <summary>
    /// Recursively extract parameters from nested JSON structures
    /// </summary>
    private void ExtractParametersRecursive(object obj, Dictionary<string, TemplateParameter> parameters, List<object> currentKeys)
    {
        switch (obj)
        {
            case JsonElement element:
                ExtractFromJsonElement(element, parameters, currentKeys);
                break;
            case Dictionary<string, object> dict:
                ExtractFromDictionary(dict, parameters, currentKeys);
                break;
            case List<object> list:
                ExtractFromList(list, parameters, currentKeys);
                break;
            case string str when IsTemplateParameter(str):
                AddParameter(str, parameters, currentKeys);
                break;
        }
    }

    private void ExtractFromJsonElement(JsonElement element, Dictionary<string, TemplateParameter> parameters, List<object> currentKeys)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    var newKeys = new List<object>(currentKeys) { property.Name };
                    ExtractParametersRecursive(property.Value, parameters, newKeys);
                }
                break;
            case JsonValueKind.Array:
                for (int i = 0; i < element.GetArrayLength(); i++)
                {
                    var newKeys = new List<object>(currentKeys) { i };
                    ExtractParametersRecursive(element[i], parameters, newKeys);
                }
                break;
            case JsonValueKind.String:
                var str = element.GetString();
                if (str != null && IsTemplateParameter(str))
                {
                    AddParameter(str, parameters, currentKeys);
                }
                break;
        }
    }

    private void ExtractFromDictionary(Dictionary<string, object> dict, Dictionary<string, TemplateParameter> parameters, List<object> currentKeys)
    {
        foreach (var kvp in dict)
        {
            var newKeys = new List<object>(currentKeys) { kvp.Key };
            ExtractParametersRecursive(kvp.Value, parameters, newKeys);
        }
    }

    private void ExtractFromList(List<object> list, Dictionary<string, TemplateParameter> parameters, List<object> currentKeys)
    {
        for (int i = 0; i < list.Count; i++)
        {
            var newKeys = new List<object>(currentKeys) { i };
            ExtractParametersRecursive(list[i], parameters, newKeys);
        }
    }

    private bool IsTemplateParameter(string value)
    {
        return value.Contains(TemplateConstants.ParameterStartDelimiter) && 
               value.Contains(TemplateConstants.ParameterEndDelimiter);
    }

    private void AddParameter(string parameterValue, Dictionary<string, TemplateParameter> parameters, List<object> currentKeys)
    {
        var extractedParams = ExtractParameterNames(parameterValue);
        
        foreach (var param in extractedParams)
        {
            if (!parameters.ContainsKey(param))
            {
                parameters[param] = new TemplateParameter { Parameter = param };
            }

            parameters[param].Locations.Add(new ParameterLocation
            {
                Keys = new List<object>(currentKeys)
            });
        }
    }

    /// <summary>
    /// Extract parameter names from a template string like "{{param1}} and {{param2}}"
    /// </summary>
    private List<string> ExtractParameterNames(string templateString)
    {
        var pattern = @"\{\{([^}]+)\}\}";
        var matches = Regex.Matches(templateString, pattern);
        
        return matches.Select(m => m.Groups[1].Value.Trim()).ToList();
    }

    /// <summary>
    /// Replace parameters in template with actual data values
    /// Equivalent to Python's replace_parameter_with_data
    /// </summary>
    public object ReplaceParametersWithData(object template, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        // Deep clone the template first
        var templateJson = JsonSerializer.Serialize(template, _jsonOptions);
        var clonedTemplate = JsonSerializer.Deserialize<object>(templateJson, _jsonOptions);

        // Step 1: Process oneOf patterns first (like Python does)
        var processedTemplate = ProcessOneOfPatterns(clonedTemplate!, csvRow);
        
        // Step 2: Replace template parameters with actual data
        return ReplaceParametersRecursive(processedTemplate, csvRow, options);
    }

    private object ReplaceParametersRecursive(object obj, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        switch (obj)
        {
            case JsonElement element:
                return ProcessJsonElement(element, csvRow, options);
            case Dictionary<string, object> dict:
                return ProcessDictionary(dict, csvRow, options);
            case List<object> list:
                return ProcessList(list, csvRow, options);
            case string str:
                return ProcessStringValue(str, csvRow, options);
            default:
                return obj;
        }
    }

    private object ProcessJsonElement(JsonElement element, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var dict = new Dictionary<string, object>();
                foreach (var property in element.EnumerateObject())
                {
                    dict[property.Name] = ReplaceParametersRecursive(property.Value, csvRow, options);
                }
                return dict;
            case JsonValueKind.Array:
                var list = new List<object>();
                foreach (var item in element.EnumerateArray())
                {
                    list.Add(ReplaceParametersRecursive(item, csvRow, options));
                }
                return list;
            case JsonValueKind.String:
                return ProcessStringValue(element.GetString() ?? "", csvRow, options);
            case JsonValueKind.Number:
                return element.GetDecimal();
            case JsonValueKind.True:
                return true;
            case JsonValueKind.False:
                return false;
            case JsonValueKind.Null:
                return null!;
            default:
                return element;
        }
    }

    private Dictionary<string, object> ProcessDictionary(Dictionary<string, object> dict, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        var result = new Dictionary<string, object>();
        
        foreach (var kvp in dict)
        {
            var processedValue = ReplaceParametersRecursive(kvp.Value, csvRow, options);
            
            // Skip empty values - if processing resulted in null or empty string, don't include the property
            if (ShouldIncludeProperty(processedValue))
            {
                // Handle special processing for array/object parents
                if (ShouldWrapInArrayParent(kvp.Key, options))
                {
                    processedValue = WrapInArrayParent(processedValue, options.ArrayParent!);
                }
                else if (ShouldWrapInObjectParent(kvp.Key, options))
                {
                    processedValue = WrapInObjectParent(processedValue, options.ObjectParent!);
                }
                
                result[kvp.Key] = processedValue;
            }
        }
        
        return result;
    }

    /// <summary>
    /// Determine if a property should be included in the result
    /// Excludes null values, empty strings, and empty collections
    /// </summary>
    private bool ShouldIncludeProperty(object value)
    {
        if (value == null) return false;
        
        if (value is string str && string.IsNullOrWhiteSpace(str)) return false;
        
        if (value is List<object> list && list.Count == 0) return false;
        
        if (value is Dictionary<string, object> dict && dict.Count == 0) return false;
        
        return true;
    }

    private List<object> ProcessList(List<object> list, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        return list.Select(item => ReplaceParametersRecursive(item, csvRow, options))
                   .Where(ShouldIncludeProperty)
                   .ToList();
    }

    /// <summary>
    /// Process string values with template parameter replacement and type conversion
    /// Equivalent to Python's string processing with type conversions
    /// </summary>
    private object ProcessStringValue(string value, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        if (string.IsNullOrEmpty(value))
            return value;

        // Handle special tags first
        if (value.Contains(TemplateConstants.TagFileName))
        {
            return ProcessFileNameTag(value, csvRow, options);
        }

        // Handle special function calls like float(), int(), bool(), datetime_YYYY-MM-DD()
        if (HasSpecialFunctionCall(value))
        {
            return ProcessSpecialFunctionCall(value, csvRow);
        }

        // Replace namespace placeholder
        if (value.Contains("<namespace>") && !string.IsNullOrEmpty(options.SchemaNamespaceValue))
        {
            value = value.Replace("<namespace>", options.SchemaNamespaceValue);
        }

        // Replace template parameters
        var result = value;
        var pattern = @"\{\{([^}]+)\}\}";
        var matches = Regex.Matches(value, pattern);

        foreach (Match match in matches.Cast<Match>().Reverse())
        {
            var paramName = match.Groups[1].Value.Trim();
            var replacement = GetParameterValue(paramName, csvRow);
            
            if (replacement != null)
            {
                result = result.Substring(0, match.Index) + replacement + result.Substring(match.Index + match.Length);
            }
        }

        // If the entire string was a single parameter, try type conversion
        if (matches.Count == 1 && matches[0].Value == value)
        {
            return ConvertToAppropriateType(result);
        }

        return result;
    }

    /// <summary>
    /// Check if string contains special function calls
    /// </summary>
    private bool HasSpecialFunctionCall(string value)
    {
        return value.Contains("float(") || value.Contains("int(") || 
               value.Contains("bool(") || value.Contains("datetime_YYYY-MM-DD(");
    }

    /// <summary>
    /// Process special function calls like float({{param}}), int({{param}}), etc.
    /// Equivalent to Python's function call processing
    /// </summary>
    private object ProcessSpecialFunctionCall(string value, Dictionary<string, string> csvRow)
    {
        // Handle float() function
        var floatMatch = Regex.Match(value, @"float\(\{\{([^}]+)\}\}\)");
        if (floatMatch.Success)
        {
            var paramName = floatMatch.Groups[1].Value.Trim();
            var paramValue = GetParameterValue(paramName, csvRow);
            if (paramValue != null && decimal.TryParse(paramValue, NumberStyles.Float, CultureInfo.InvariantCulture, out var floatResult))
            {
                return floatResult;
            }
            return null;
        }

        // Handle int() function
        var intMatch = Regex.Match(value, @"int\(\{\{([^}]+)\}\}\)");
        if (intMatch.Success)
        {
            var paramName = intMatch.Groups[1].Value.Trim();
            var paramValue = GetParameterValue(paramName, csvRow);
            if (paramValue != null && int.TryParse(paramValue, out var intResult))
            {
                return intResult;
            }
            return null;
        }

        // Handle bool() function
        var boolMatch = Regex.Match(value, @"bool\(\{\{([^}]+)\}\}\)");
        if (boolMatch.Success)
        {
            var paramName = boolMatch.Groups[1].Value.Trim();
            var paramValue = GetParameterValue(paramName, csvRow);
            if (paramValue != null && bool.TryParse(paramValue, out var boolResult))
            {
                return boolResult;
            }
            return null;
        }

        // Handle datetime_YYYY-MM-DD() function
        var dateMatch = Regex.Match(value, @"datetime_YYYY-MM-DD\(\{\{([^}]+)\}\}\)");
        if (dateMatch.Success)
        {
            var paramName = dateMatch.Groups[1].Value.Trim();
            var paramValue = GetParameterValue(paramName, csvRow);
            if (paramValue != null && DateTime.TryParse(paramValue, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateResult))
            {
                return dateResult.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            }
            return null;
        }

        // If no special function found, process normally
        return ProcessStringValue(value.Replace("float(", "").Replace("int(", "").Replace("bool(", "").Replace("datetime_YYYY-MM-DD(", "").Replace(")", ""), csvRow, new TemplateProcessingOptions());
    }

    private string? GetParameterValue(string paramName, Dictionary<string, string> csvRow)
    {
        // Handle array indexing like "param[0]"
        if (paramName.Contains('[') && paramName.Contains(']'))
        {
            return HandleArrayParameter(paramName, csvRow);
        }

        // Direct parameter lookup
        return csvRow.TryGetValue(paramName, out var value) ? value : null;
    }

    private string? HandleArrayParameter(string paramName, Dictionary<string, string> csvRow)
    {
        var match = Regex.Match(paramName, @"^([^[]+)\[(\d+)\]$");
        if (!match.Success)
            return null;

        var baseParam = match.Groups[1].Value;
        var index = int.Parse(match.Groups[2].Value);

        if (!csvRow.TryGetValue(baseParam, out var arrayValue) || string.IsNullOrEmpty(arrayValue))
            return null;

        // Try to parse as JSON array
        try
        {
            var array = JsonSerializer.Deserialize<string[]>(arrayValue);
            return array != null && index < array.Length ? array[index] : null;
        }
        catch
        {
            // If not JSON, treat as comma-separated
            var parts = arrayValue.Split(',');
            return index < parts.Length ? parts[index].Trim() : null;
        }
    }

    /// <summary>
    /// Convert string values to appropriate types (int, float, bool, datetime)
    /// Equivalent to Python's type conversion logic
    /// </summary>
    private object ConvertToAppropriateType(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;

        // Try boolean conversion
        if (bool.TryParse(value, out var boolResult))
            return boolResult;

        // Try integer conversion
        if (int.TryParse(value, out var intResult))
            return intResult;

        // Try decimal conversion
        if (decimal.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var decimalResult))
            return decimalResult;

        // Try datetime conversion (ISO 8601 format)
        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateResult))
            return dateResult.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");

        // Return as string if no conversion possible
        return value;
    }

    private string ProcessFileNameTag(string value, Dictionary<string, string> csvRow, TemplateProcessingOptions options)
    {
        if (!string.IsNullOrEmpty(options.GroupFilename))
        {
            return options.GroupFilename;
        }

        // Extract filename from template
        var pattern = @"\$filename\{([^}]+)\}";
        var match = Regex.Match(value, pattern);
        
        if (match.Success)
        {
            var filenameTemplate = match.Groups[1].Value;
            return ProcessStringValue(filenameTemplate, csvRow, options).ToString() ?? "manifest";
        }

        return "manifest";
    }

    private bool ShouldWrapInArrayParent(string key, TemplateProcessingOptions options)
    {
        return !string.IsNullOrEmpty(options.ArrayParent) && key == options.ArrayParent;
    }

    private bool ShouldWrapInObjectParent(string key, TemplateProcessingOptions options)
    {
        return !string.IsNullOrEmpty(options.ObjectParent) && key == options.ObjectParent;
    }

    private object WrapInArrayParent(object value, string arrayParentKey)
    {
        return new Dictionary<string, object>
        {
            [arrayParentKey] = new List<object> { value }
        };
    }

    private object WrapInObjectParent(object value, string objectParentKey)
    {
        return new Dictionary<string, object>
        {
            [objectParentKey] = value
        };
    }

    /// <summary>
    /// Extract the array parent value from template before special tags are removed
    /// Equivalent to reading "$array_parent" from Python templates
    /// </summary>
    public string? ExtractArrayParent(object template)
    {
        return ExtractArrayParentRecursive(template);
    }

    private string? ExtractArrayParentRecursive(object obj)
    {
        switch (obj)
        {
            case Dictionary<string, object> dict:
                // Look for $array_parent in this dictionary
                if (dict.TryGetValue("$array_parent", out var arrayParentValue))
                {
                    return arrayParentValue?.ToString();
                }
                
                // Recursively search in nested objects
                foreach (var kvp in dict)
                {
                    var result = ExtractArrayParentRecursive(kvp.Value);
                    if (result != null)
                        return result;
                }
                break;
                
            case List<object> list:
                // Search in each item of the list
                foreach (var item in list)
                {
                    var result = ExtractArrayParentRecursive(item);
                    if (result != null)
                        return result;
                }
                break;
                
            case JsonElement element:
                if (element.ValueKind == JsonValueKind.Object)
                {
                    // Look for $array_parent property
                    if (element.TryGetProperty("$array_parent", out var arrayParentProperty))
                    {
                        return arrayParentProperty.GetString();
                    }
                    
                    // Recursively search in nested objects
                    foreach (var property in element.EnumerateObject())
                    {
                        var result = ExtractArrayParentRecursive(property.Value);
                        if (result != null)
                            return result;
                    }
                }
                else if (element.ValueKind == JsonValueKind.Array)
                {
                    // Search in each item of the array
                    foreach (var item in element.EnumerateArray())
                    {
                        var result = ExtractArrayParentRecursive(item);
                        if (result != null)
                            return result;
                    }
                }
                break;
        }
        
        return null;
    }

    /// <summary>
    /// Remove special tags from the processed manifest
    /// Equivalent to Python's remove_special_tags
    /// </summary>
    public object RemoveSpecialTags(object manifest)
    {
        return RemoveSpecialTagsRecursive(manifest);
    }

    private object RemoveSpecialTagsRecursive(object obj)
    {
        switch (obj)
        {
            case Dictionary<string, object> dict:
                var result = new Dictionary<string, object>();
                foreach (var kvp in dict)
                {
                    if (!IsSpecialTag(kvp.Key))
                    {
                        result[kvp.Key] = RemoveSpecialTagsRecursive(kvp.Value);
                    }
                }
                return result;
            case List<object> list:
                return list.Select(RemoveSpecialTagsRecursive).ToList();
            case string str:
                return RemoveSpecialTagsFromString(str);
            default:
                return obj;
        }
    }

    private bool IsSpecialTag(string key)
    {
        return key.StartsWith("$$_oneOf_") || key.StartsWith("$$_anyOf_") || key.StartsWith("$");
    }

    /// <summary>
    /// Process oneOf patterns in template - keeps only the first valid oneOf option
    /// Equivalent to Python's oneOf processing logic
    /// </summary>
    public object ProcessOneOfPatterns(object template, Dictionary<string, string> csvRow)
    {
        return ProcessOneOfRecursive(template, csvRow);
    }

    private object ProcessOneOfRecursive(object obj, Dictionary<string, string> csvRow)
    {
        switch (obj)
        {
            case Dictionary<string, object> dict:
                return ProcessOneOfInDictionary(dict, csvRow);
            case List<object> list:
                return list.Select(item => ProcessOneOfRecursive(item, csvRow)).ToList();
            case JsonElement element:
                return ProcessOneOfInJsonElement(element, csvRow);
            default:
                return obj;
        }
    }

    private object ProcessOneOfInDictionary(Dictionary<string, object> dict, Dictionary<string, string> csvRow)
    {
        var result = new Dictionary<string, object>();
        
        // Group properties by their oneOf patterns
        var oneOfGroups = new Dictionary<string, List<KeyValuePair<string, object>>>();
        var normalProperties = new List<KeyValuePair<string, object>>();
        
        foreach (var kvp in dict)
        {
            if (kvp.Key.Contains("$$_oneOf_"))
            {
                // Extract the base property name (remove the oneOf suffix)
                var match = Regex.Match(kvp.Key, @"(.+?)\$\$_oneOf_(\d+)\$\$(.*)");
                if (match.Success)
                {
                    var baseName = match.Groups[1].Value + match.Groups[3].Value;
                    var oneOfIndex = match.Groups[2].Value;
                    var groupKey = $"{baseName}";
                    
                    if (!oneOfGroups.ContainsKey(groupKey))
                    {
                        oneOfGroups[groupKey] = new List<KeyValuePair<string, object>>();
                    }
                    oneOfGroups[groupKey].Add(new KeyValuePair<string, object>(kvp.Key, kvp.Value));
                }
            }
            else
            {
                normalProperties.Add(kvp);
            }
        }
        
        // Process normal properties
        foreach (var kvp in normalProperties)
        {
            result[kvp.Key] = ProcessOneOfRecursive(kvp.Value, csvRow);
        }
        
        // Process oneOf groups - select the first one that has data
        foreach (var group in oneOfGroups)
        {
            var selectedOption = SelectBestOneOfOption(group.Value, csvRow);
            if (selectedOption.HasValue)
            {
                // Extract the final property name
                var match = Regex.Match(selectedOption.Value.Key, @"(.+?)\$\$_oneOf_\d+\$\$(.*)");
                if (match.Success)
                {
                    var finalPropertyName = match.Groups[1].Value + match.Groups[2].Value;
                    result[finalPropertyName] = ProcessOneOfRecursive(selectedOption.Value.Value, csvRow);
                }
            }
        }
        
        return result;
    }

    private KeyValuePair<string, object>? SelectBestOneOfOption(List<KeyValuePair<string, object>> options, Dictionary<string, string> csvRow)
    {
        // Sort by oneOf index and select the first one that has corresponding data in CSV
        var sortedOptions = options.OrderBy(opt =>
        {
            var match = Regex.Match(opt.Key, @"\$\$_oneOf_(\d+)\$\$");
            return match.Success ? int.Parse(match.Groups[1].Value) : int.MaxValue;
        }).ToList();
        
        foreach (var option in sortedOptions)
        {
            if (HasDataForOption(option.Value, csvRow))
            {
                return option;
            }
        }
        
        // If no option has data, return the first one
        return sortedOptions.FirstOrDefault();
    }

    private bool HasDataForOption(object optionValue, Dictionary<string, string> csvRow)
    {
        // Check if this option has template parameters that exist in CSV data
        var parameters = ExtractTemplateParameters(optionValue);
        
        foreach (var param in parameters)
        {
            if (csvRow.ContainsKey(param.Parameter) && !string.IsNullOrEmpty(csvRow[param.Parameter]))
            {
                return true;
            }
        }
        
        return false;
    }

    private object ProcessOneOfInJsonElement(JsonElement element, Dictionary<string, string> csvRow)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var dict = new Dictionary<string, object>();
                foreach (var property in element.EnumerateObject())
                {
                    dict[property.Name] = property.Value;
                }
                return ProcessOneOfInDictionary(dict, csvRow);
            case JsonValueKind.Array:
                var list = new List<object>();
                foreach (var item in element.EnumerateArray())
                {
                    list.Add(ProcessOneOfRecursive(item, csvRow));
                }
                return list;
            default:
                return element;
        }
    }

    private string RemoveSpecialTagsFromString(string value)
    {
        // Remove oneOf and anyOf patterns
        value = Regex.Replace(value, TemplateConstants.PatternOneOf, "");
        value = Regex.Replace(value, TemplateConstants.PatternAnyOf, "");
        
        return value;
    }
}
