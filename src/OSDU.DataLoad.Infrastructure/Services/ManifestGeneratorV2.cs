using CsvHelper;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace OSDU.DataLoad.Infrastructure.Services
{
    using CsvHelper;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using OSDU.DataLoad.Domain.Entities;
    using OSDU.DataLoad.Domain.Interfaces;
    using System.Globalization;
    using System.Text.Json;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml;

    public class ManifestGeneratorV2 : IManifestGenerator
    {
        private const string ParameterStartDelimiter = "{{";
        private const string ParameterEndDelimiter = "}}";
        private const string TagFileName = "$filename";
        private const string TagSchemaId = "$schema";
        private const string TagArrayParent = "$array_parent";
        private const string TagObjectParent = "$object_parent";
        private const string TagKindParent = "$kind_parent";
        private const string TagRequiredTemplate = "$required_template";
        private const string TagOptionalField = "$";

        private const string PatternOneOf = @"\$\$_oneOf_[1-9][0-9]*\$\$";
        private const string PatternAnyOf = @"\$\$_anyOf_[1-9][0-9]*\$\$";
        private static readonly Regex PatternRemove = new Regex($"({PatternOneOf}|{PatternAnyOf})");

        private readonly ILogger<ManifestGeneratorV2> _logger;

        public ManifestGeneratorV2(ILogger<ManifestGeneratorV2> logger)
        {
            _logger = logger;
        }

        public Dictionary<string, JObject> LoadSchemas(string schemaPath, string schemaNsName = null, string schemaNsValue = null)
        {
            var dictSchemas = new Dictionary<string, JObject>();
            var fileList = new List<string>();

            ListSchemaFiles(schemaPath, fileList);

            foreach (var schemaFile in fileList)
            {
                var jsonText = File.ReadAllText(schemaFile);
                var schema = JObject.Parse(jsonText);

                if (!string.IsNullOrEmpty(schemaNsName))
                {
                    schema = ReplaceJsonNamespace(schema, $"{schemaNsName}:", $"{schemaNsValue}:");
                }

                var id = schema["$id"]?.ToString() ?? schema["$ID"]?.ToString();
                if (!string.IsNullOrEmpty(id))
                {
                    dictSchemas[id] = schema;
                }

                // For top level resource
                if (schema["properties"]?["ResourceHomeRegionID"] != null)
                {
                    // Remove 'required'
                    if (schema["required"] != null)
                    {
                        schema.Remove("required");
                    }
                    // Remove 'additionalProperties'
                    if (schema["additionalProperties"] != null)
                    {
                        schema.Remove("additionalProperties");
                    }
                    // Remove resource/type id version
                    var resourceTypeIdPattern = schema["properties"]?["ResourceTypeID"]?["pattern"]?.ToString();
                    if (!string.IsNullOrEmpty(resourceTypeIdPattern))
                    {
                        schema["properties"]["ResourceTypeID"]["pattern"] = resourceTypeIdPattern.Replace(":[0-9]+", ":[0-9]*");
                    }

                    var resourceIdPattern = schema["properties"]?["ResourceID"]?["pattern"]?.ToString();
                    if (!string.IsNullOrEmpty(resourceIdPattern))
                    {
                        schema["properties"]["ResourceID"]["pattern"] = resourceIdPattern.Replace(":[0-9]+", ":[0-9]*");
                    }
                }
            }

            // Resolve latest version
            ResolveLatestVersions(dictSchemas);

            return dictSchemas;
        }

        private void ListSchemaFiles(string path, List<string> fileList)
        {
            var files = Directory.GetFileSystemEntries(path);
            foreach (var file in files)
            {
                if (File.Exists(file) && file.EndsWith(".json"))
                {
                    fileList.Add(file);
                }
                else if (Directory.Exists(file))
                {
                    ListSchemaFiles(file, fileList);
                }
            }
        }

        private void ResolveLatestVersions(Dictionary<string, JObject> dictSchemas)
        {
            var dictLatestKey = new Dictionary<string, string>();
            var dictLatestVersion = new Dictionary<string, int>();

            foreach (var kvp in dictSchemas.ToList())
            {
                var key = kvp.Key;
                var keyParts = key.Split('/');

                if (keyParts.Length > 1 && int.TryParse(keyParts[^1], out var keyVersion))
                {
                    var keyLatestId = string.Join("/", keyParts[..^1]) + "/";

                    if (!dictLatestVersion.ContainsKey(keyLatestId) || keyVersion > dictLatestVersion[keyLatestId])
                    {
                        dictLatestVersion[keyLatestId] = keyVersion;
                        dictLatestKey[keyLatestId] = key;
                    }
                }
            }

            foreach (var kvp in dictLatestKey)
            {
                dictSchemas[kvp.Key] = dictSchemas[kvp.Value];
            }
        }

        private JObject ReplaceJsonNamespace(JObject jsonObj, string nsName, string nsValue)
        {
            if (string.IsNullOrEmpty(nsName))
                return jsonObj;

            var jsonStr = jsonObj.ToString();
            var replacedJson = jsonStr.Replace(nsName, nsValue);
            return JObject.Parse(replacedJson);
        }

        public void ParseTemplateParameters(JToken rootObject, Dictionary<string, List<ParameterRecord>> parametersObject)
        {
            ParseDict(rootObject, rootObject, new List<string>(), new List<(JToken, List<string>)>(), parametersObject);
        }

        private void ParseDict(JToken parentObject, JToken rootObject, List<string> keys,
            List<(JToken, List<string>)> rootList, Dictionary<string, List<ParameterRecord>> parametersObject)
        {
            if (parentObject is JObject obj)
            {
                foreach (var property in obj.Properties())
                {
                    var newKeys = new List<string>(keys) { property.Name };

                    if (property.Value is JObject)
                    {
                        ParseDict(property.Value, rootObject, newKeys, rootList, parametersObject);
                    }
                    else if (property.Value is JArray array && array.Count == 1)
                    {
                        var newRootList = new List<(JToken, List<string>)>(rootList)
                        {
                            (rootObject, newKeys)
                        };
                        var newRootObject = new JObject { ["0"] = array[0] };
                        ParseDict(newRootObject, newRootObject, new List<string>(), newRootList, parametersObject);
                    }
                    else if (property.Value is JValue val && val.Type == JTokenType.String)
                    {
                        ParseStr(rootObject, newKeys, val.ToString(), new List<(JToken, List<string>)>(rootList), parametersObject);
                    }
                }
            }
        }

        private void ParseStr(JToken rootObject, List<string> keys, string val,
            List<(JToken, List<string>)> rootList, Dictionary<string, List<ParameterRecord>> parametersObject)
        {
            val = val.Trim();
            var parameterStartIndex = val.IndexOf(ParameterStartDelimiter);
            var parameterEndIndex = val.IndexOf(ParameterEndDelimiter);

            if (parameterStartIndex >= 0 && parameterEndIndex >= 0)
            {
                var parameter = val.Substring(parameterStartIndex, parameterEndIndex + ParameterEndDelimiter.Length - parameterStartIndex);

                if (!string.IsNullOrEmpty(parameter))
                {
                    if (!parametersObject.ContainsKey(parameter))
                    {
                        parametersObject[parameter] = new List<ParameterRecord>();
                    }
                    parametersObject[parameter].Add(new ParameterRecord(rootObject, keys, rootList));
                }

                var valLeft = val.Substring(parameterEndIndex + ParameterEndDelimiter.Length);
                if (!string.IsNullOrEmpty(valLeft))
                {
                    ParseStr(rootObject, keys, valLeft, rootList, parametersObject);
                }
            }
        }

        public Dictionary<string, object> MapCsvColumnNamesToParameters(string csvFile,
            Dictionary<string, List<ParameterRecord>> parametersObject)
        {
            var columnNames = new List<string>();

            using var reader = new StreamReader(csvFile);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            csv.Read();
            csv.ReadHeader();
            columnNames = csv.HeaderRecord.Select(h => h.Trim().ToLower()).ToList();

            var mapParameterColumn = new Dictionary<string, object>();

            _logger.LogInformation("CSV Columns found: {Columns}", string.Join(", ", columnNames));
            _logger.LogInformation("Template Parameters found: {Parameters}", string.Join(", ", parametersObject.Keys));

            foreach (var kvp in parametersObject)
            {
                var parameter = kvp.Key;
                var parameterRecs = kvp.Value;
                var parameterKey = parameter.Substring(ParameterStartDelimiter.Length,
                    parameter.Length - ParameterStartDelimiter.Length - ParameterEndDelimiter.Length).Trim().ToLower();

                _logger.LogInformation("Processing parameter: {Parameter} -> key: {ParameterKey}", parameter, parameterKey);

                foreach (var parameterRec in parameterRecs)
                {
                    var dArray = parameterRec.RootList.Count;
                    _logger.LogInformation("Parameter {Parameter} has dArray: {DArray}", parameter, dArray);

                    if (dArray == 0)
                    {
                        // Parameters not inside array type
                        if (!mapParameterColumn.ContainsKey(parameter))
                        {
                            var csvIndex = GetColumnIndex(columnNames, parameterKey);
                            if (csvIndex >= 0)
                            {
                                mapParameterColumn[parameter] = csvIndex;
                                _logger.LogInformation("Mapped simple parameter {Parameter} to column index {Index}", parameter, csvIndex);
                            }
                        }
                    }
                    else
                    {
                        // Parameters inside array type
                        if (mapParameterColumn.ContainsKey(parameter))
                        {
                            throw new Exception($"Duplicate array parameter not allowed: {parameter}");
                        }

                        var parameterColumn = new List<List<int>>();
                        mapParameterColumn[parameter] = parameterColumn;

                        // Find max indexes
                        var indexesCount = new int[dArray];
                        var pattern = Regex.Escape(parameterKey);
                        for (int i = 0; i < dArray; i++)
                        {
                            pattern += @"_[1-9][0-9]*";
                        }

                        _logger.LogInformation("Array parameter {Parameter} pattern: {Pattern}", parameter, pattern);

                        foreach (var columnName in columnNames)
                        {
                            if (Regex.IsMatch(columnName, $"^{pattern}$"))
                            {
                                _logger.LogInformation("Found matching column for array parameter: {Column}", columnName);
                                var tempName = columnName.Substring(parameterKey.Length + 1);
                                var colNums = tempName.Split('_').Select(int.Parse).ToArray();
                                for (int i = 0; i < dArray; i++)
                                {
                                    indexesCount[i] = Math.Max(indexesCount[i], colNums[i]);
                                }
                            }
                        }

                        _logger.LogInformation("Array parameter {Parameter} max indexes: {Indexes}", parameter, string.Join(",", indexesCount));

                        // Find csv column index for each parameter array indexes
                        GenerateArrayParameterColumns(parameterKey, columnNames, indexesCount, parameterColumn);

                        _logger.LogInformation("Array parameter {Parameter} mapped to {Count} column combinations", parameter, parameterColumn.Count);
                    }
                }
            }

            return mapParameterColumn;
        }

        private int GetColumnIndex(List<string> colNames, string colName)
        {
            var idx = colNames.IndexOf(colName);
            if (idx >= 0)
            {
                try
                {
                    var duplicateIdx = colNames.IndexOf(colName, idx + 1);
                    if (duplicateIdx >= 0)
                    {
                        throw new Exception($"Duplicate parameter found in csv file: {colName}");
                    }
                }
                catch (ArgumentOutOfRangeException)
                {
                    // No duplicate found, which is expected
                }
            }
            return idx;
        }

        private void GenerateArrayParameterColumns(string parameterKey, List<string> columnNames,
            int[] indexesCount, List<List<int>> parameterColumn)
        {
            var dArray = indexesCount.Length;
            var indexesColumn = new int[dArray + 1];

            bool done = false;
            while (!done)
            {
                var parameterWithIndexes = parameterKey;
                for (int i = 0; i < dArray; i++)
                {
                    parameterWithIndexes += $"_{indexesColumn[i] + 1}";
                }

                var csvIndex = GetColumnIndex(columnNames, parameterWithIndexes);
                if (csvIndex >= 0)
                {
                    indexesColumn[dArray] = csvIndex;
                    parameterColumn.Add(new List<int>(indexesColumn));
                }

                // Increment counters
                for (int i = dArray - 1; i >= 0; i--)
                {
                    var tempVal = indexesColumn[i] + 1;
                    if (tempVal < indexesCount[i])
                    {
                        for (int j = dArray - 1; j >= i; j--)
                        {
                            indexesColumn[j] = 0;
                        }
                        indexesColumn[i] = tempVal;
                        break;
                    }
                    done = (i == 0);
                }
            }
        }

        private (JToken, object) GetDeepestKeyObject(JToken rootObject, List<string> keys)
        {
            if (keys == null || keys.Count == 0)
            {
                var wrapper = new JObject { ["0"] = rootObject };
                return (wrapper, "0");
            }

            JToken parentObject = rootObject;
            object key = keys[0];

            for (int i = 1; i < keys.Count; i++)
            {
                if (parentObject is JObject obj && obj.ContainsKey(keys[i - 1]))
                {
                    parentObject = obj[keys[i - 1]];
                }
                else if (parentObject is JArray arr && int.TryParse(keys[i - 1], out var index) && index < arr.Count)
                {
                    parentObject = arr[index];
                }
                key = keys[i];
            }

            // Convert string keys to integers when parent is JArray
            if (parentObject is JArray && key is string strKey && int.TryParse(strKey, out var intKey))
            {
                key = intKey;
            }

            return (parentObject, key);
        }

        private void ReplaceParameterWithData(JToken rootObject, List<string> keys, string parameter, string[] dataRow, int? colIndex)
        {
            var data = "";
            if (colIndex.HasValue && colIndex.Value >= 0 && colIndex.Value < dataRow.Length)
            {
                data = dataRow[colIndex.Value].Trim();
            }

            _logger.LogInformation("ReplaceParameterWithData: parameter={Parameter}, colIndex={ColIndex}, data='{Data}', keys={Keys}", parameter, colIndex, data, string.Join(".", keys));

            if (string.IsNullOrEmpty(data))
            {
                _logger.LogInformation("Skipping empty data for parameter {Parameter}", parameter);
                return; // Will clean up later
            }

            var (parentObject, key) = GetDeepestKeyObject(rootObject, keys);
            _logger.LogInformation("GetDeepestKeyObject result: parentObject type={ParentType}, key={Key}", parentObject?.GetType().Name, key);

            string originalValue;
            if (parentObject is JObject obj && key is string strKey)
            {
                originalValue = obj[strKey]?.ToString() ?? "";
                _logger.LogInformation("Found JObject property: {Key} = '{Value}'", strKey, originalValue);
            }
            else if (parentObject is JArray arr && key is int intKey)
            {
                originalValue = arr[intKey]?.ToString() ?? "";
                _logger.LogInformation("Found JArray element: [{Index}] = '{Value}'", intKey, originalValue);
            }
            else
            {
                _logger.LogWarning("Unable to find parent object for parameter {Parameter}. ParentType={ParentType}, Key={Key}, KeyType={KeyType}", parameter, parentObject?.GetType().Name, key, key?.GetType().Name);
                return;
            }

            var result = originalValue.Replace(parameter, data);
            _logger.LogInformation("Parameter replacement: '{Original}' -> '{Result}'", originalValue, result);

            // Check if this needs to be evaluated
            var escapedParameter = Regex.Escape(parameter);
            var pattern = $@"(int|float|bool|datetime_YYYY-MM-DD|datetime_MM/DD/YYYY)\({escapedParameter}\)";

            if (Regex.IsMatch(originalValue.Trim(), $"^{pattern}$"))
            {
                if (result.StartsWith("bool"))
                {
                    var boolValue = new[] { "true", "yes", "y", "t", "1" }.Contains(data.ToLower());
                    SetValue(parentObject, key, boolValue);
                }
                else if (result.StartsWith("datetime_YYYY-MM-DD"))
                {
                    if (DateTime.TryParseExact(data, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var inputDateTime))
                    {
                        SetValue(parentObject, key, inputDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ"));
                    }
                }
                else if (result.StartsWith("datetime_MM/DD/YYYY"))
                {
                    if (DateTime.TryParseExact(data, "MM/dd/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out var inputDateTime))
                    {
                        SetValue(parentObject, key, inputDateTime.ToString("yyyy-MM-ddTHH:mm:ss.fffK"));
                    }
                }
                else if (result.StartsWith("int"))
                {
                    if (int.TryParse(data, out var intValue))
                    {
                        SetValue(parentObject, key, intValue);
                    }
                }
                else if (result.StartsWith("float"))
                {
                    if (double.TryParse(data, out var floatValue))
                    {
                        SetValue(parentObject, key, floatValue);
                    }
                }
            }
            else
            {
                SetValue(parentObject, key, result);
            }
        }

        private void SetValue(JToken parentObject, object key, object value)
        {
            if (parentObject is JObject obj && key is string strKey)
            {
                obj[strKey] = JToken.FromObject(value);
            }
            else if (parentObject is JArray arr && key is int intKey)
            {
                arr[intKey] = JToken.FromObject(value);
            }
        }

        private (JToken, List<string>) GetRootObject(List<(JToken, List<string>)> rootObjectKeyList, List<int> indexes)
        {
            var (rootObject, rootKeys) = rootObjectKeyList[0];
            var (parentObject, key) = GetDeepestKeyObject(rootObject, rootKeys);

            JArray arrayObject;
            if (parentObject is JObject obj && key is string strKey)
            {
                if (obj[strKey] is not JArray arr)
                {
                    obj[strKey] = new JArray();
                }
                arrayObject = (JArray)obj[strKey];
            }
            else if (parentObject is JArray arr && key is int intKey)
            {
                if (arr[intKey] is not JArray subArr)
                {
                    arr[intKey] = new JArray();
                }
                arrayObject = (JArray)arr[intKey];
            }
            else
            {
                return (null, null);
            }

            JToken returnRoot = null;
            List<string> returnKeys = null;

            for (int i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                var (childObjectTemplate, childKeys) = rootObjectKeyList[i + 1];

                if (childKeys.Count > 0 && childKeys[0] == "0")
                {
                    if (childObjectTemplate is JObject templateObj && templateObj.ContainsKey("0"))
                    {
                        childObjectTemplate = templateObj["0"];
                    }
                    childKeys = childKeys.Skip(1).ToList();
                }

                while (arrayObject.Count <= index)
                {
                    arrayObject.Add(childObjectTemplate.DeepClone());
                }

                returnRoot = arrayObject[index];
                returnKeys = childKeys;

                if (returnRoot.Type == JTokenType.String)
                {
                    returnRoot = arrayObject;
                    returnKeys = new List<string> { index.ToString() };
                }

                var (nextParentObject, nextKey) = GetDeepestKeyObject(returnRoot, returnKeys);
                if (nextParentObject is JObject nextObj && nextKey is string nextStrKey)
                {
                    if (nextObj[nextStrKey] is not JArray nextArr)
                    {
                        nextObj[nextStrKey] = new JArray();
                    }
                    arrayObject = (JArray)nextObj[nextStrKey];
                }
                else if (nextParentObject is JArray nextArr && nextKey is int nextIntKey)
                {
                    if (nextArr[nextIntKey] is not JArray subArr)
                    {
                        nextArr[nextIntKey] = new JArray();
                    }
                    arrayObject = (JArray)nextArr[nextIntKey];
                }
            }

            return (returnRoot, returnKeys);
        }

        private void ClearNonFilledParameters(JToken ldm)
        {
            ClearDict(ldm);
        }

        private void ClearDict(JToken token)
        {
            if (token is JObject obj)
            {
                var toBeDeleted = new List<string>();

                foreach (var property in obj.Properties().ToList())
                {
                    var key = property.Name;
                    var val = property.Value;

                    if (val is JObject)
                    {
                        ClearDict(val);
                        if (!val.HasValues)
                        {
                            toBeDeleted.Add(key);
                        }
                    }
                    else if (val is JArray arr)
                    {
                        ClearList(arr);
                        if (arr.Count == 0)
                        {
                            toBeDeleted.Add(key);
                        }
                    }
                    else if (val is JValue jval && jval.Type == JTokenType.String)
                    {
                        var strVal = jval.ToString();
                        var parameterStartIndex = strVal.IndexOf(ParameterStartDelimiter);
                        var parameterEndIndex = strVal.IndexOf(ParameterEndDelimiter);
                        if (parameterStartIndex >= 0 && parameterEndIndex >= 0)
                        {
                            toBeDeleted.Add(key);
                        }
                    }
                }

                foreach (var delKey in toBeDeleted)
                {
                    obj.Remove(delKey);
                }
            }
            else if (token is JArray arr)
            {
                ClearList(arr);
            }
        }

        private void ClearList(JArray lst)
        {
            for (int i = lst.Count - 1; i >= 0; i--)
            {
                var listVal = lst[i];
                if (listVal is JObject)
                {
                    ClearDict(listVal);
                }
                else if (listVal is JArray)
                {
                    ClearList((JArray)listVal);
                }
                else if (listVal is JValue jval && jval.Type == JTokenType.String)
                {
                    var strVal = jval.ToString();
                    var parameterStartIndex = strVal.IndexOf(ParameterStartDelimiter);
                    var parameterEndIndex = strVal.IndexOf(ParameterEndDelimiter);
                    if (parameterStartIndex >= 0 && parameterEndIndex >= 0)
                    {
                        lst.RemoveAt(i);
                    }
                }
            }

            // Remove extra empty items in list
            for (int i = lst.Count - 1; i >= 0; i--)
            {
                if (IsItemEmpty(lst[i]))
                {
                    lst.RemoveAt(i);
                }
            }
        }

        private bool IsItemEmpty(JToken item)
        {
            if (item is JObject obj)
            {
                return !obj.HasValues || obj.Properties().All(p => IsItemEmpty(p.Value));
            }
            else if (item is JArray arr)
            {
                return arr.Count == 0 || arr.All(IsItemEmpty);
            }
            else
            {
                return false;
            }
        }

        private void RemoveSpecialTags(JToken ldm)
        {
            RemoveDictTags(ldm);
        }

        private void RemoveDictTags(JToken token)
        {
            if (token is JObject obj)
            {
                var toBeReplaced = new Dictionary<string, string>();

                foreach (var property in obj.Properties().ToList())
                {
                    var key = property.Name;
                    var newKey = PatternRemove.Replace(key, "");
                    if (newKey != key)
                    {
                        toBeReplaced[key] = newKey;
                    }

                    var val = property.Value;
                    if (val is JObject || val is JArray)
                    {
                        RemoveDictTags(val);
                    }
                }

                foreach (var kvp in toBeReplaced)
                {
                    if (obj.ContainsKey(kvp.Value))
                    {
                        throw new Exception($"Detected duplicate attributes: {kvp.Key}");
                    }
                    var value = obj[kvp.Key];
                    obj.Remove(kvp.Key);
                    obj[kvp.Value] = value;
                }
            }
            else if (token is JArray arr)
            {
                RemoveListTags(arr);
            }
        }

        private void RemoveListTags(JArray lst)
        {
            foreach (var item in lst)
            {
                if (item is JObject || item is JArray)
                {
                    RemoveDictTags(item);
                }
            }
        }

        private void AddRequiredFields(JToken ldm, JToken requiredTemplate)
        {
            if (requiredTemplate == null || !requiredTemplate.HasValues)
            {
                return;
            }

            AddFieldsDict(ldm, requiredTemplate);
        }

        private void AddFieldsDict(JToken desToken, JToken srcToken)
        {
            if (desToken is JObject desDict && srcToken is JObject srcDict)
            {
                foreach (var srcProperty in srcDict.Properties())
                {
                    var k = srcProperty.Name;
                    var vSrc = srcProperty.Value;
                    var optionalField = false;

                    if (k.StartsWith(TagOptionalField))
                    {
                        optionalField = true;
                        k = k.Substring(TagOptionalField.Length);
                    }

                    if (!desDict.ContainsKey(k) && !optionalField)
                    {
                        if (vSrc is JObject)
                        {
                            desDict[k] = new JObject();
                        }
                        else if (vSrc is JArray)
                        {
                            desDict[k] = new JArray();
                        }
                        else
                        {
                            desDict[k] = vSrc.DeepClone();
                        }
                    }

                    var vDes = desDict[k];
                    if (vDes != null && vSrc.Type == vDes.Type)
                    {
                        if (vSrc is JObject)
                        {
                            AddFieldsDict(vDes, vSrc);
                        }
                        else if (vSrc is JArray)
                        {
                            AddFieldsList((JArray)vDes, (JArray)vSrc);
                        }
                    }
                }
            }
        }

        private void AddFieldsList(JArray desList, JArray srcList)
        {
            if (srcList.Count == 0 || desList.Count == 0)
            {
                return;
            }

            foreach (var itemSrc in srcList)
            {
                foreach (var itemDes in desList)
                {
                    if (itemSrc.Type == itemDes.Type)
                    {
                        if (itemSrc is JObject)
                        {
                            AddFieldsDict(itemDes, itemSrc);
                        }
                        else if (itemSrc is JArray)
                        {
                            AddFieldsList((JArray)itemDes, (JArray)itemSrc);
                        }
                    }
                }
            }
        }

        private void SetAclValues(JToken manifest, string aclViewer, string aclOwner)
        {
            if (manifest is not JObject obj)
                return;

            // Check for existing ACL object (try both cases)
            JObject acl = null;
            string aclKey = null;

            if (obj.ContainsKey("Acl"))
            {
                acl = obj["Acl"] as JObject;
                aclKey = "Acl";
            }
            else if (obj.ContainsKey("acl"))
            {
                acl = obj["acl"] as JObject;
                aclKey = "acl";
            }
            else
            {
                // Create new ACL object with capitalized key to match required template
                acl = new JObject();
                aclKey = "Acl";
                obj[aclKey] = acl;
            }

            if (acl == null)
                return;

            // Set viewers (check for both cases)
            if (!string.IsNullOrEmpty(aclViewer))
            {
                JArray viewers = null;
                string viewersKey = null;

                if (acl.ContainsKey("Viewers"))
                {
                    viewers = acl["Viewers"] as JArray;
                    viewersKey = "Viewers";
                }
                else if (acl.ContainsKey("viewers"))
                {
                    viewers = acl["viewers"] as JArray;
                    viewersKey = "viewers";
                }
                else
                {
                    // Use capitalized key to match required template
                    viewers = new JArray();
                    viewersKey = "Viewers";
                    acl[viewersKey] = viewers;
                }

                if (viewers != null && !viewers.Any(v => v.ToString() == aclViewer))
                {
                    viewers.Add(aclViewer);
                }
            }

            // Set owners (check for both cases)
            if (!string.IsNullOrEmpty(aclOwner))
            {
                JArray owners = null;
                string ownersKey = null;

                if (acl.ContainsKey("Owners"))
                {
                    owners = acl["Owners"] as JArray;
                    ownersKey = "Owners";
                }
                else if (acl.ContainsKey("owners"))
                {
                    owners = acl["owners"] as JArray;
                    ownersKey = "owners";
                }
                else
                {
                    // Use capitalized key to match required template
                    owners = new JArray();
                    ownersKey = "Owners";
                    acl[ownersKey] = owners;
                }

                if (owners != null && !owners.Any(o => o.ToString() == aclOwner))
                {
                    owners.Add(aclOwner);
                }
            }
        }

        public JToken CreateManifestFromRow(JToken rootTemplate, JToken requiredTemplate,
            Dictionary<string, int> csvHeaders, string[] dataRow, string aclViewer = null, string aclOwner = null)
        {
            // Create new instance from template
            var ldm = rootTemplate.DeepClone();
            
            // Map CSV data directly to JSON structure
            MapCsvDataToJson(ldm, csvHeaders, dataRow);

            // Clean up non-filled parameters
            ClearNonFilledParameters(ldm);

            // Clear special tags
            RemoveSpecialTags(ldm);

            // Add required fields if needed
            if (requiredTemplate != null)
            {
                AddRequiredFields(ldm, requiredTemplate);
            }

            // Set ACL values if provided
            SetAclValues(ldm, aclViewer, aclOwner);

            return ldm;
        }

        private Dictionary<string, int> GetCsvHeaders(CsvReader csv)
        {
            var headers = new Dictionary<string, int>();
            
            if (csv.HeaderRecord != null)
            {
                for (int i = 0; i < csv.HeaderRecord.Length; i++)
                {
                    var columnName = csv.HeaderRecord[i].Trim();
                    if (!string.IsNullOrEmpty(columnName))
                    {
                        headers[columnName] = i;
                    }
                }
            }
            
            return headers;
        }

        private void MapCsvDataToJson(JToken json, Dictionary<string, int> csvHeaders, string[] dataRow)
        {
            foreach (var header in csvHeaders)
            {
                var columnName = header.Key;
                var columnIndex = header.Value;
                
                if (columnIndex < 0 || columnIndex >= dataRow.Length)
                    continue;
                    
                var value = dataRow[columnIndex]?.Trim();
                if (string.IsNullOrEmpty(value))
                    continue;

                _logger.LogInformation("Mapping CSV column {ColumnName} (index {Index}) = '{Value}'", columnName, columnIndex, value);
                
                SetJsonValueFromCsvColumn(json, columnName, value);
            }
        }

        private void SetJsonValueFromCsvColumn(JToken json, string columnName, string value)
        {
            try
            {
                // Handle array parameters with indexes (e.g., "acl_owners_1", "data_namealiases_aliasname_1")
                if (Regex.IsMatch(columnName, @".*_\d+$"))
                {
                    SetArrayValueFromCsvColumn(json, columnName, value);
                }
                else
                {
                    // Handle simple parameters
                    SetSimpleValueFromCsvColumn(json, columnName, value);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to set value for column {ColumnName}", columnName);
            }
        }

        private void SetArrayValueFromCsvColumn(JToken json, string columnName, string value)
        {
            // Parse array column name like "acl_owners_1" or "data_namealiases_aliasname_2"
            var parts = columnName.Split('_');
            var arrayIndex = int.Parse(parts.Last()) - 1; // Convert to 0-based index
            var baseName = string.Join("_", parts.Take(parts.Length - 1));
            
            // Find the JSON path for this array element
            var jsonPath = ConvertColumnNameToJsonPath(baseName);
            _logger.LogInformation("Array path: {Path}, index: {Index}, value: {Value}", jsonPath, arrayIndex, value);
            
            // Navigate to the array and set the value
            var pathParts = jsonPath.Split('.');
            JToken current = json;
            
            // Navigate to the parent of the array
            for (int i = 0; i < pathParts.Length - 1; i++)
            {
                var part = pathParts[i];
                if (current is JObject obj)
                {
                    if (!obj.ContainsKey(part))
                    {
                        obj[part] = new JObject();
                    }
                    current = obj[part];
                }
            }
            
            // Handle the final array property
            var arrayPropertyName = pathParts.Last();
            if (current is JObject parentObj)
            {
                if (!parentObj.ContainsKey(arrayPropertyName))
                {
                    parentObj[arrayPropertyName] = new JArray();
                }
                
                var array = parentObj[arrayPropertyName] as JArray;
                if (array != null)
                {
                    // Ensure array has enough elements
                    while (array.Count <= arrayIndex)
                    {
                        array.Add(new JValue((string)null));
                    }
                    
                    array[arrayIndex] = JValue.CreateString(value);
                    _logger.LogInformation("Set array[{Index}] = {Value}", arrayIndex, value);
                }
            }
        }

        private void SetSimpleValueFromCsvColumn(JToken json, string columnName, string value)
        {
            var jsonPath = ConvertColumnNameToJsonPath(columnName);
            _logger.LogInformation("Simple path: {Path}, value: {Value}", jsonPath, value);
            
            var pathParts = jsonPath.Split('.');
            JToken current = json;
            
            // Navigate to the parent
            for (int i = 0; i < pathParts.Length - 1; i++)
            {
                var part = pathParts[i];
                if (current is JObject obj)
                {
                    if (!obj.ContainsKey(part))
                    {
                        obj[part] = new JObject();
                    }
                    current = obj[part];
                }
            }
            
            // Set the final property
            if (current is JObject finalObj)
            {
                finalObj[pathParts.Last()] = JValue.CreateString(value);
                _logger.LogInformation("Set {Property} = {Value}", pathParts.Last(), value);
            }
        }

        private string ConvertColumnNameToJsonPath(string columnName)
        {
            // Convert CSV column names to JSON paths
            // Examples:
            // "id" -> "id"
            // "kind" -> "kind" 
            // "acl_owners" -> "acl.owners"
            // "data_source" -> "data.Source"
            // "data_facilityname" -> "data.FacilityName"
            // "data_namealiases_aliasname" -> "data.NameAliases.AliasName"
            
            var normalized = columnName.ToLower();
            
            // Handle special cases
            if (normalized == "id") return "id";
            if (normalized == "kind") return "kind";
            if (normalized == "acl_owners") return "acl.owners";
            if (normalized == "acl_viewers") return "acl.viewers";
            
            // Handle data prefix
            if (normalized.StartsWith("data_"))
            {
                var withoutData = normalized.Substring(5); // Remove "data_"
                var path = "data." + ConvertToPropertyCase(withoutData);
                return path;
            }
            
            // Handle meta prefix  
            if (normalized.StartsWith("meta$$_oneof_"))
            {
                // Handle meta array elements
                return "meta"; // Simplified for now
            }
            
            return ConvertToPropertyCase(normalized);
        }

        private string ConvertToPropertyCase(string input)
        {
            // Convert snake_case to PascalCase for JSON properties
            // Examples:
            // "facilityname" -> "FacilityName"
            // "namealiases_aliasname" -> "NameAliases.AliasName"
            // "spatiallocation_wgs84coordinates_type" -> "SpatialLocation.Wgs84Coordinates.type"
            
            var parts = input.Split('_');
            var result = new List<string>();
            
            foreach (var part in parts)
            {
                if (string.IsNullOrEmpty(part)) continue;
                
                // Special cases for known property names
                if (part == "wgs84coordinates") result.Add("Wgs84Coordinates");
                else if (part == "namealiases") result.Add("NameAliases");
                else if (part == "spatiallocation") result.Add("SpatialLocation");
                else if (part == "facilityoperators") result.Add("FacilityOperators");
                else if (part == "facilitystates") result.Add("FacilityStates");
                else if (part == "facilityevents") result.Add("FacilityEvents");
                else if (part == "verticalmeasurements") result.Add("VerticalMeasurements");
                else if (part == "geocontexts") result.Add("GeoContexts");
                else if (part == "type") result.Add("type"); // Keep lowercase for GeoJSON
                else if (part == "features") result.Add("features"); // Keep lowercase for GeoJSON
                else if (part == "geometry") result.Add("geometry"); // Keep lowercase for GeoJSON
                else if (part == "coordinates") result.Add("coordinates"); // Keep lowercase for GeoJSON
                else
                {
                    // Capitalize first letter
                    result.Add(char.ToUpper(part[0]) + part.Substring(1));
                }
            }
            
            return string.Join(".", result);
        }
        private async Task<ManifestMappingConfig?> LoadMappingConfigAsync(string mappingFilePath, CancellationToken cancellationToken)
        {
            try
            {
                var jsonOptions = new JsonSerializerOptions
                {
                    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                    WriteIndented = true,
                    PropertyNameCaseInsensitive = true
                };
                var json = await File.ReadAllTextAsync(mappingFilePath, cancellationToken);
                return JsonSerializer.Deserialize<ManifestMappingConfig>(json, jsonOptions);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load mapping configuration from: {MappingFile}", mappingFilePath);
                return null;
            }
        }

        public async Task<bool> GenerateManifestsFromCsvAsync(
            string mappingFilePath,
            string templateType,
            string dataDirectory,
            string outputDirectory,
            string homeDirectory,
            string dataPartition,
            string aclViewer,
            string aclOwner,
            string legalTag = null,
            bool groupFile = false,
            CancellationToken cancellationToken = default)
        {
            try
            {
                _logger.LogInformation("Enhanced manifest generation from mapping file: {MappingFile}", mappingFilePath);

                var mappingConfig = await LoadMappingConfigAsync(mappingFilePath, cancellationToken);
                if (mappingConfig == null)
                    return false;

                Directory.CreateDirectory(outputDirectory);

                var templatesDir = Path.Combine(homeDirectory, "templates", templateType);
                var csvDataDir = Path.Combine(homeDirectory, "TNO", "contrib", dataDirectory);

                foreach (var mapping in mappingConfig.Mapping)
                {
                    var csvFilePath = Path.Combine(csvDataDir, mapping.DataFile);
                    var templateFilePath = Path.Combine(templatesDir, mapping.TemplateFile);

                    if (!File.Exists(csvFilePath) || !File.Exists(templateFilePath))
                    {
                        _logger.LogWarning("Missing file - CSV: {CsvExists}, Template: {TemplateExists}",
                            File.Exists(csvFilePath), File.Exists(templateFilePath));
                        continue;
                    }

                    _logger.LogInformation("Processing mapping: {DataFile} -> {TemplateFile}", mapping.DataFile, mapping.TemplateFile);

                    try
                    {
                        // Call CreateManifestFromCsv with proper parameters
                        CreateManifestFromCsv(
                            inputCsv: csvFilePath,
                            templateJson: templateFilePath,
                            outputPath: outputDirectory,
                            dataPartition: dataPartition,
                            requiredTemplate: mappingConfig.RequiredTemplate != null ? JsonConvert.SerializeObject(mappingConfig.RequiredTemplate) : null,
                            arrayParent: null, // Will be extracted from template during processing
                            objectParent: null, // Will be extracted from template during processing  
                            groupFilename: groupFile ? mapping.OutputFileName : null,
                            aclViewer: aclViewer,
                            aclOwner: aclOwner,
                            legalTag: legalTag
                        );

                        _logger.LogInformation("Successfully processed mapping for {DataFile}", mapping.DataFile);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to process mapping for {DataFile}", mapping.DataFile);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in enhanced manifest generation");
                return false;
            }
        }

        public void CreateManifestFromCsv(string inputCsv, string templateJson, string outputPath,
            string dataPartition = null,
            string requiredTemplate = null,
            string arrayParent = null,
            string objectParent = null,
            string groupFilename = null,
            string aclViewer = null,
            string aclOwner = null,
            string legalTag = null)
        {
            try
            {
                _logger.LogInformation("Calling Python script to generate manifests from CSV: {InputCsv}", inputCsv);
                
                // Build arguments for Python script
                var args = new List<string>
                {
                    inputCsv,
                    templateJson,
                    outputPath
                };

                string tempRequiredTemplateFile = null;

                // Add optional parameters
                if (!string.IsNullOrEmpty(dataPartition))
                {
                    args.Add($"--schema-ns-value={dataPartition}");
                }

                if (!string.IsNullOrEmpty(requiredTemplate))
                {
                    // Write required template to a temporary file to avoid command line escaping issues
                    tempRequiredTemplateFile = Path.GetTempFileName();
                    File.WriteAllText(tempRequiredTemplateFile, requiredTemplate);
                    args.Add($"--required-template-file={tempRequiredTemplateFile}");
                }

                if (!string.IsNullOrEmpty(arrayParent))
                {
                    args.Add($"--array-parent={arrayParent}");
                }

                if (!string.IsNullOrEmpty(objectParent))
                {
                    args.Add($"--object-parent={objectParent}");
                }

                if (!string.IsNullOrEmpty(groupFilename))
                {
                    args.Add($"--group-filename={groupFilename}");
                }

                if (!string.IsNullOrEmpty(aclViewer))
                {
                    args.Add($"--acl-viewer={aclViewer}");
                }

                if (!string.IsNullOrEmpty(aclOwner))
                {
                    args.Add($"--acl-owner={aclOwner}");
                }

                if (!string.IsNullOrEmpty(legalTag))
                {
                    args.Add($"--legal-tag={legalTag}");
                }

                try
                {
                    // Call Python script
                    CallPythonScript(args.ToArray());
                    
                    _logger.LogInformation("Successfully generated manifests using Python script");
                }
                finally
                {
                    // Clean up temporary file
                    if (tempRequiredTemplateFile != null && File.Exists(tempRequiredTemplateFile))
                    {
                        try
                        {
                            File.Delete(tempRequiredTemplateFile);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Failed to delete temporary file: {TempFile}", tempRequiredTemplateFile);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to generate manifests using Python script");
                throw;
            }
        }

        private void CallPythonScript(string[] args)
        {
            try
            {
                // Find the Python script path relative to the repository root
                // Current directory when running dotnet run is: c:\Users\kymille\src\energy\osdu-data-load-tno\src
                // We need to go up one level to reach the repository root: c:\Users\kymille\src\energy\osdu-data-load-tno
                var currentDir = Directory.GetCurrentDirectory();
                var repoRoot = Directory.GetParent(currentDir)?.FullName ?? "";
                var scriptPath = Path.Combine(repoRoot, "generate-manifest-scripts", "csv_to_json_wrapper.py");

                _logger.LogInformation("Current directory: {CurrentDir}", currentDir);
                _logger.LogInformation("Repository root: {RepoRoot}", repoRoot);
                _logger.LogInformation("Looking for script at: {ScriptPath}", scriptPath);

                if (!File.Exists(scriptPath))
                {
                    throw new FileNotFoundException($"Python script not found at: {scriptPath}");
                }

                _logger.LogInformation("Using Python script: {ScriptPath}", scriptPath);
                var scriptArgs = string.Join(" ", args.Select(arg => $"\"{arg}\""));
                var processInfo = new ProcessStartInfo
                {
                    FileName = "python",
                    Arguments = $"\"{scriptPath}\" {scriptArgs}",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true,
                    WorkingDirectory = Path.GetDirectoryName(scriptPath) // Set working directory to script location
                };

                _logger.LogInformation("Executing: {FileName} {Arguments}", processInfo.FileName, processInfo.Arguments);

                using var process = new Process();
                process.StartInfo = processInfo;

                // Set up event handlers for real-time output streaming
                process.OutputDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        _logger.LogInformation("[Python Output] {Data}", e.Data);
                    }
                };

                process.ErrorDataReceived += (sender, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                    {
                        _logger.LogWarning("[Python Error] {Data}", e.Data);
                    }
                };

                process.Start();

                // Begin asynchronous read operations
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();

                // Wait for the process to complete
                process.WaitForExit();

                _logger.LogInformation("Python script finished with exit code: {ExitCode}", process.ExitCode);

                if (process.ExitCode != 0)
                {
                    _logger.LogError("Python script failed with exit code {ExitCode}", process.ExitCode);
                    throw new InvalidOperationException($"Python script failed with exit code: {process.ExitCode}");
                }

                _logger.LogInformation("Python script completed successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calling Python script");
                throw;
            }
        }

        private JToken GetAndRemoveProperty(JToken token, string propertyName)
        {
            if (token is JObject obj && obj.ContainsKey(propertyName))
            {
                var value = obj[propertyName];
                obj.Remove(propertyName);
                return value;
            }
            return null;
        }

        private JToken WrapInArrayParent(JToken lm, string arrayParent, string kindParent)
        {
            var parentItems = arrayParent.Split(".");
            var newLm = new JObject();

            if (!string.IsNullOrEmpty(kindParent))
            {
                newLm["kind"] = kindParent;
            }

            var lmParent = newLm;
            for (int i = 0; i < parentItems.Length - 1; i++)
            {
                var parentItem = parentItems[i].Trim();
                lmParent[parentItem] = new JObject();
                lmParent = (JObject)lmParent[parentItem];
            }
            lmParent[parentItems[^1]] = new JArray { lm };

            return newLm;
        }

        private JToken WrapInObjectParent(JToken lm, string objectParent, string kindParent)
        {
            var parentItems = objectParent.Split(".");
            var newLm = new JObject();

            if (!string.IsNullOrEmpty(kindParent))
            {
                newLm["kind"] = kindParent;
            }

            var lmParent = newLm;
            for (int i = 0; i < parentItems.Length - 1; i++)
            {
                var parentItem = parentItems[i].Trim();
                lmParent[parentItem] = new JObject();
                lmParent = (JObject)lmParent[parentItem];
            }
            lmParent[parentItems[^1]] = lm;

            return newLm;
        }

        private void HandleDuplicateFiles(ref string outputFile, HashSet<string> processedLower)
        {
            var duplicateNameCount = 1;
            while (processedLower.Contains(outputFile.ToLower()))
            {
                var nameParts = outputFile.Split('.');
                if (nameParts.Length > 1)
                {
                    nameParts[^2] = $"{nameParts[^2]}_{duplicateNameCount}";
                    outputFile = string.Join(".", nameParts);
                }
                else
                {
                    outputFile = $"{outputFile}_{duplicateNameCount}";
                }
                duplicateNameCount++;
            }
        }
    }

    public class ParameterRecord
    {
        public JToken RootObject { get; }
        public List<string> Keys { get; }
        public List<(JToken, List<string>)> RootList { get; }

        public ParameterRecord(JToken rootObject, List<string> keys, List<(JToken, List<string>)> rootList)
        {
            RootObject = rootObject;
            Keys = new List<string>(keys);
            RootList = new List<(JToken, List<string>)>(rootList);
        }
    }
}