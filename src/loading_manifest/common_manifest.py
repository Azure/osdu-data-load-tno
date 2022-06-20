#  Copyright © Microsoft Corporation
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Common constants and functions"""

import os
import csv
from zipfile import ZipFile
from io import BytesIO
import json
import jsonschema
from urllib.parse import quote
import logging


class ResourceSecurityClassification:
    """Resource security classification constants"""

    CLASSIFIED = "<namespace>:reference-data--ResourceSecurityClassification:CLASSIFIED:"
    CONFIDENTIAL = "<namespace>:reference-data--ResourceSecurityClassification:CONFIDENTIAL:"
    MOST_CONFIDENTIAL = "<namespace>:reference-data--ResourceSecurityClassification:MOST-CONFIDENTIAL:"
    RESTRICTED = "<namespace>:reference-data--ResourceSecurityClassification:RESTRICTED:"


class LineageAssertionsRelationshipType:
    """LineageAssertions RelationshipType constants"""

    PREDECESOR = "PREDECESSOR"
    SOURCE = "SOURCE"
    REFERENCE = "REFERENCE"


class LoadingManifest:
    """Loading manifest constants"""

    TAG_REFERENCE_DATA = "ReferenceData"
    TAG_MASTER_DATA = "MasterData"
    TAG_WP_DATA = "Data"
    TAG_KIND = "kind"
    TAG_WORK_PRODUCT = "WorkProduct"
    TAG_WORK_PRODUCT_COMPONENTS = "WorkProductComponents"
    TAG_DATASETS = "Datasets"

    KIND_ID_1_0_0 = "<namespace>:wks:Manifest:1.0.0"

    SCHEMA_ID_1_0_0 = "https://schema.osdu.opengroup.org/json/manifest/Manifest.1.0.0.json"


class WorkProductManifest:
    """Work product manifest constants"""

    TAG_KIND = "kind"
    # TAG_GROUP_TYPE = "groupType"
    TAG_ACL = "acl"
    TAG_LEGAL = "legal"
    TAG_RESOURCE_SECURITY_CLASSIFICATION = "ResourceSecurityClassification"
    TAG_DATA = "data"
    TAG_DATA_COMPONENTS = "Components"
    TAG_DATA_NAME = "Name"
    TAG_DATA_DESCRIPTION = "Description"
    TAG_DATA_EXTENSION_PROPERTIES = "ExtensionProperties"
    TAG_COMPONENTS_ASSOCIATIVE_IDS = "ComponentsAssociativeIDs"

    KIND_GENERIC_WORK_PRODUCT_1_0_0 = "<namespace>:wks:work-product--WorkProduct:1.0.0"
    # GROUP_TYPE_WORK_PRODUCT = "work-product"
    SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0 = "https://schema.osdu.opengroup.org/json/work-product/WorkProduct.1.0.0.json"

    DESCRIPTION_DOCUMENT = "Document"
    DESCRIPTION_WELL_LOG = "Well Log"
    DESCRIPTION_WELLBORE_PATH = "Wellbore Trajectory"
    DESCRIPTION_WELLBORE_TOP = "Wellbore Marker"


class WorkProductComponentManifest:
    """Work product component manifest constants"""

    TAG_ID = "id"
    TAG_KIND = "kind"
    # TAG_GROUP_TYPE = "groupType"
    TAG_ACL = "acl"
    TAG_LEGAL = "legal"
    TAG_META = "meta"
    TAG_META_KIND = "kind"
    TAG_META_NAME = "name"
    TAG_META_PERSIST_REF = "persistableReference"
    TAG_META_UOM = "unitOfMeasureID"
    TAG_META_PROPERTY_NAMES = "propertyNames"
    TAG_RESOURCE_SECURITY_CLASSIFICATION = "ResourceSecurityClassification"
    TAG_DATA = "data"
    TAG_DATA_DATASETS = "Datasets"
    TAG_DATA_ARTEFACTS = "Artefacts"
    TAG_DATA_NAME = "Name"
    TAG_DATA_DESCRIPTION = "Description"
    TAG_DATA_AUTHOR_IDS = "AuthorIDs"
    TAG_DATA_CREATION_DATETIME = "CreationDateTime"
    TAG_DATA_WELL_BORE_ID = "WellboreID"
    TAG_DATA_WELL_LOG_TYPE_ID = "WellLogTypeID"
    TAG_DATA_SERVICE_COMPANY_ID = "ServiceCompanyID"
    TAG_DATA_LOG_SERVICE_DATE = "LogServiceDate"
    TAG_DATA_LOG_QUALITY = "LogQuality"
    TAG_DATA_LOG_SOURCE = "LogSource"
    TAG_DATA_LOG_ACTIVITY = "LogActivity"
    TAG_DATA_LOG_VERSION = "LogVersion"
    TAG_DATA_SAMPLING_INTERVAL = "SamplingInterval"
    TAG_DATA_SAMPLING_START = "SamplingStart"
    TAG_DATA_SAMPLING_STOP = "SamplingStop"
    TAG_DATA_TOP_DEPTH = "TopMeasuredDepth"
    # TAG_DATA_TOP_DEPTH_DEPTH = "Depth"
    # TAG_DATA_TOP_DEPTH_UOM = "UnitOfMeasure"
    TAG_DATA_BOTTOM_DEPTH = "BottomMeasuredDepth"
    # TAG_DATA_BOTTOM_DEPTH_DEPTH = "Depth"
    # TAG_DATA_BOTTOM_DEPTH_UOM = "UnitOfMeasure"
    TAG_DATA_TOOL_DESC = "ToolStringDescription"
    TAG_DATA_CURVES = "Curves"
    TAG_DATA_CURVES_SERVICE_COMPANY_ID = "ServiceCompanyID"
    TAG_DATA_CURVES_LOG_CURVE_BV_ID = "LogCurveBusinessValueID"
    TAG_DATA_CURVES_MNEMONIC = "Mnemonic"
    TAG_DATA_CURVES_LOG_CURVE_TYPE_ID = "LogCurveTypeID"
    TAG_DATA_CURVES_CURVE_QUALITY = "CurveQuality"
    TAG_DATA_CURVES_INTERPRETER_NAME = "InterpreterName"
    TAG_DATA_CURVES_TOP_DEPTH = "TopDepth"
    TAG_DATA_CURVES_BASE_DEPTH = "BaseDepth"
    TAG_DATA_CURVES_DEPTH_UNIT = "DepthUnit"
    TAG_DATA_CURVES_CURVE_UNIT = "CurveUnit"
    TAG_DATA_CURVES_NUMBER_OF_COLUMNS = "NumberOfColumns"
    TAG_DATA_MERKERS = "Markers"
    TAG_DATA_MERKERS_PICK_NAME = "PickName"
    TAG_DATA_MERKERS_MARKER_NAME = "MarkerName"
    TAG_DATA_MERKERS_PICK_DEPTH = "PickDepth"
    TAG_DATA_MERKERS_MARKER_DEPTH = "MarkerMeasuredDepth"
    TAG_DATA_DEPTH_UNIT = "DepthUnit"
    TAG_DATA_TOP_MD = "TopDepthMeasuredDepth"
    TAG_DATA_BASE_MD = "BaseDepthMeasuredDepth"
    TAG_DATA_VERTICAL_MEASUREMENT = "VerticalMeasurement"
    TAG_DATA_LINEAGE_ASSERTIONS = "LineageAssertions"
    TAG_DATA_LINEAGE_ASSERTIONS_ID = "ID"
    TAG_DATA_LINEAGE_ASSERTIONS_RELATIONSHIP_TYPE = "RelationshipType"
    TAG_FILE_ASSOCIATIVE_IDS = "FileAssociativeIDs"
    TAG_DATA_EXTENSION_PROPERTIES = "ExtensionProperties"
    TAG_ASSOCIATIVE_ID = "AssociativeID"

    KIND_DOCUMENT_1_0_0 = "<namespace>:wks:work-product-component--Document:1.0.0"
    KIND_WELL_LOG_1_0_0 = "<namespace>:wks:work-product-component--WellLog:1.0.0"
    KIND_WELL_LOG_1_1_0 = "<namespace>:wks:work-product-component--WellLog:1.1.0"
    KIND_WELLBORE_PATH_1_0_0 = "<namespace>:wks:work-product-component--WellboreTrajectory:1.0.0"
    KIND_WELLBORE_PATH_1_1_0 = "<namespace>:wks:work-product-component--WellboreTrajectory:1.1.0"
    KIND_WELLBORE_TOP_1_0_0 = "<namespace>:wks:work-product-component--WellboreMarkerSet:1.0.0"
    # GROUP_TYPE_WORK_PRODUCT_COMPONENT = "work-product-component"

    WELL_BORE_ID = "<namespace>:master-data--Wellbore:"
    WELL_LOG_TYPE_ID = "<namespace>:reference-data--LogType:"
    SERVICE_COMPANY_ID = "<namespace>:master-data--Organisation:"
    LOG_CURVE_TYPE_ID = "<namespace>:reference-data--LogCurveType:"
    LOG_CURVE_BV_ID = "<namespace>:reference-data--LogCurveBusinessValue:"
    UOM_ID = "<namespace>:reference-data--UnitOfMeasure:"

    META_KIND_UNIT = "Unit"

    DESCRIPTION_DOCUMENT = "Document"
    DESCRIPTION_WELL_LOG = "Well Log"
    DESCRIPTION_WELLBORE_PATH = "Wellbore Trajectory"
    DESCRIPTION_WELLBORE_TOP = "Wellbore Marker"

    SCHEMA_ID_DOCUMENT_1_0_0 = "https://schema.osdu.opengroup.org/json/work-product-component/Document.1.0.0.json"
    SCHEMA_ID_WELL_LOG_1_0_0 = "https://schema.osdu.opengroup.org/json/work-product-component/WellLog.1.0.0.json"
    SCHEMA_ID_WELL_LOG_1_1_0 = "https://schema.osdu.opengroup.org/json/work-product-component/WellLog.1.1.0.json"
    SCHEMA_ID_WELLBORE_TOP_1_0_0 = \
        "https://schema.osdu.opengroup.org/json/work-product-component/WellboreMarkerSet.1.0.0.json"
    SCHEMA_ID_WELLBORE_PATH_1_0_0 = \
        "https://schema.osdu.opengroup.org/json/work-product-component/WellboreTrajectory.1.0.0.json"
    SCHEMA_ID_WELLBORE_PATH_1_1_0 = \
        "https://schema.osdu.opengroup.org/json/work-product-component/WellboreTrajectory.1.1.0.json"


class FileManifest:
    """File manifest constants"""

    TAG_ID = "id"
    TAG_KIND = "kind"
    # TAG_GROUP_TYPE = "groupType"
    TAG_ACL = "acl"
    TAG_LEGAL = "legal"
    TAG_RESOURCE_SECURITY_CLASSIFICATION = "ResourceSecurityClassification"
    TAG_DATA = "data"
    TAG_DATA_SCHEMA_FORMAT_TYPE_ID = "SchemaFormatTypeID"
    TAG_DATA_PRELOAD_FILE_PATH = "PreloadFilePath"
    TAG_DATA_NAME = "Name"
    TAG_DATA_FILE_SOURCE = "FileSource"
    TAG_DATA_EXTENSION_PROPERTIES = "ExtensionProperties"
    TAG_ASSOCIATIVE_ID = "AssociativeID"
    TAG_DATA_DATASET_PROPERTIES = "DatasetProperties"
    TAG_DATA_DATASET_PROPERTIES_FS_INFO = "FileSourceInfo"

    SCHEMA_FORMAT_TYPE_ID_CVS = "<namespace>:reference-data--SchemaFormatType:TabSeparatedColumnarText:"
    SCHEMA_FORMAT_TYPE_ID_LAS2 = "<namespace>:reference-data--SchemaFormatType:LAS2:"

    KIND_ID_GENERIC_DATASET_1_0_0 = "<namespace>:wks:dataset--File.Generic:1.0.0"

    TYPE_ID_DOC = "<namespace>:wks:file.MSO-doc:"
    TYPE_ID_DOCX = "<namespace>:wks:file.MSO-docx:"
    TYPE_ID_XLS = "<namespace>:wks:file.MSO-xls:"
    TYPE_ID_XLSX = "<namespace>:wks:file.MSO-xlsx:"
    TYPE_ID_PPT = "<namespace>:wks:file.MSO-ppt:"
    TYPE_ID_PPTX = "<namespace>:wks:file.MSO-pptx:"
    TYPE_ID_PPTM = "<namespace>:wks:file.MSO-pptm:"
    TYPE_ID_CSV = "<namespace>:wks:file.csv:"

    TYPE_ID_CLI = "<namespace>:wks:file.SLB-cli:"
    TYPE_ID_PDS = "<namespace>:wks:file.SLB-pds:"

    SCHEMA_ID_GENERIC_DATASET_1_0_0 = "https://schema.osdu.opengroup.org/json/dataset/File.Generic.1.0.0.json"


def create_loading_manifest(
        work_product,
        work_product_components,
        files,
        schema_id_lm,
        schema_id_wp,
        schema_id_wpc,
        schema_id_file,
        schema_ns_name,
        schema_ns_value,
        dict_schemas):
    lm = dict()
    lm[LoadingManifest.TAG_WORK_PRODUCT] = work_product
    lm[LoadingManifest.TAG_WORK_PRODUCT_COMPONENTS] = work_product_components
    lm[LoadingManifest.TAG_DATASETS] = files

    lm_group = dict()
    lm_group[LoadingManifest.TAG_KIND] = LoadingManifest.KIND_ID_1_0_0
    lm_group[LoadingManifest.TAG_REFERENCE_DATA] = []
    lm_group[LoadingManifest.TAG_MASTER_DATA] = []
    lm_group[LoadingManifest.TAG_WP_DATA] = lm

    if schema_ns_name is not None and len(schema_ns_name) > 0:
        lm_group = replace_json_namespace(lm_group, schema_ns_name + ":", schema_ns_value + ":")

    if dict_schemas is not None and len(dict_schemas) > 0:
        if schema_id_lm is not None and len(schema_id_lm) > 0:
            validate_schema(lm_group, schema_id_lm, dict_schemas)
        if schema_id_wp is not None and len(schema_id_wp) > 0:
            wp = replace_json_namespace(work_product, schema_ns_name + ":", schema_ns_value + ":")
            del wp[WorkProductManifest.TAG_DATA][WorkProductManifest.TAG_DATA_COMPONENTS]
            validate_schema(wp, schema_id_wp, dict_schemas)
        if schema_id_wpc is not None and len(schema_id_wpc) > 0:
            for wpc in work_product_components:
                wpc = replace_json_namespace(wpc, schema_ns_name + ":", schema_ns_value + ":")
                del wpc[WorkProductComponentManifest.TAG_ID]
                wpc_data = wpc[WorkProductComponentManifest.TAG_DATA]
                del wpc_data[WorkProductComponentManifest.TAG_DATA_DATASETS]
                validate_schema(wpc, schema_id_wpc, dict_schemas)
        if schema_id_file is not None and len(schema_id_file) > 0:
            for file in files:
                file = replace_json_namespace(file, schema_ns_name + ":", schema_ns_value + ":")
                del file[FileManifest.TAG_ID]
                validate_schema(file, schema_id_file, dict_schemas)

    return lm_group


def create_work_product_manifest(
        kind,
        acl,
        legal,
        resource_security_classificaton,
        name,
        description):
    wp = dict()
    wp[WorkProductManifest.TAG_KIND] = kind
    wp[WorkProductManifest.TAG_ACL] = acl
    wp[WorkProductManifest.TAG_LEGAL] = legal

    wp_data = dict()
    wp[WorkProductManifest.TAG_DATA] = wp_data

    wp_data[WorkProductManifest.TAG_RESOURCE_SECURITY_CLASSIFICATION] \
        = resource_security_classificaton

    wp_data[WorkProductManifest.TAG_DATA_NAME] = name
    wp_data[WorkProductManifest.TAG_DATA_DESCRIPTION] = description
    wp_data[WorkProductManifest.TAG_DATA_COMPONENTS] = list()

    # wp_data[WorkProductManifest.TAG_DATA_EXTENSION_PROPERTIES] = dict()

    return wp


def create_work_product_component_manifest(
        kind,
        acl,
        legal,
        resource_security_classificaton,
        name,
        description):
    wpc = dict()
    wpc[WorkProductComponentManifest.TAG_ID] = ""
    wpc[WorkProductComponentManifest.TAG_KIND] = kind
    wpc[WorkProductComponentManifest.TAG_ACL] = acl
    wpc[WorkProductComponentManifest.TAG_LEGAL] = legal
    wpc[WorkProductComponentManifest.TAG_META] = []

    wpc_data = dict()
    wpc[WorkProductComponentManifest.TAG_DATA] = wpc_data

    wpc_data[WorkProductComponentManifest.TAG_RESOURCE_SECURITY_CLASSIFICATION] \
        = resource_security_classificaton

    wpc_data[WorkProductComponentManifest.TAG_DATA_NAME] = name
    wpc_data[WorkProductComponentManifest.TAG_DATA_DESCRIPTION] = description
    wpc_data[WorkProductComponentManifest.TAG_DATA_DATASETS] = list()

    # wpc_data[WorkProductComponentManifest.TAG_DATA_EXTENSION_PROPERTIES] = dict()

    return wpc


def create_file_manifest(
        kind,
        schema_format_type,
        acl,
        legal,
        resource_security_classificaton,
        preload_file_path,
        file_source,
        file_name=None):
    file = dict()
    file[FileManifest.TAG_ID] = ""
    file[FileManifest.TAG_KIND] = kind
    file[FileManifest.TAG_ACL] = acl
    file[FileManifest.TAG_LEGAL] = legal

    file_data = dict()
    file[FileManifest.TAG_DATA] = file_data

    file_data[FileManifest.TAG_RESOURCE_SECURITY_CLASSIFICATION] \
        = resource_security_classificaton

    if schema_format_type is not None and len(schema_format_type) > 0:
        file_data[FileManifest.TAG_DATA_SCHEMA_FORMAT_TYPE_ID] = schema_format_type

    ds_props = dict()
    ds_props_fs_info = dict()
    if file_source is not None and len(file_source) > 0:
        ds_props_fs_info[FileManifest.TAG_DATA_FILE_SOURCE] = file_source
    else:
        ds_props_fs_info[FileManifest.TAG_DATA_FILE_SOURCE] = preload_file_path
    if file_name is not None:
        ds_props_fs_info[FileManifest.TAG_DATA_NAME] = file_name
    ds_props_fs_info[FileManifest.TAG_DATA_PRELOAD_FILE_PATH] = preload_file_path
    ds_props[FileManifest.TAG_DATA_DATASET_PROPERTIES_FS_INFO] = ds_props_fs_info
    file_data[FileManifest.TAG_DATA_DATASET_PROPERTIES] = ds_props

    return file


def associate_work_product_components(
        work_product,
        work_product_components):
    """wipe out any existing association"""

    associative_ids = work_product[WorkProductManifest.TAG_DATA][WorkProductManifest.TAG_DATA_COMPONENTS]

    count = 0
    for wpc in work_product_components:
        count = count + 1
        associative_id = "surrogate-key:wpc-" + str(count)
        associative_ids.append(associative_id)
        wpc[WorkProductComponentManifest.TAG_ID] = associative_id


def associate_files(
        work_product_component,
        files,
        starting_index=0):
    """wipe out any existing association"""

    associative_ids = work_product_component[WorkProductComponentManifest.TAG_DATA]\
        .get(WorkProductComponentManifest.TAG_DATA_DATASETS)

    count = starting_index
    for file in files:
        count = count + 1
        associative_id = "surrogate-key:file-" + str(count)
        associative_ids.append(associative_id)
        file[FileManifest.TAG_ID] = associative_id

    return count


def append_file_separator(file_path):
    if file_path is None or len(file_path) == 0:
        return file_path

    if file_path.endswith("/") or file_path.endswith("\\"):
        return file_path

    sep = os.sep
    if "\\" in file_path:
        sep = "\\"
    elif "/" in file_path:
        sep = "/"

    return file_path + sep


def is_nonzero_file(input_path, file_name):
    full_path = os.path.join(input_path, file_name)
    if (os.path.isfile(full_path)
            and os.stat(full_path).st_size > 0):
        return True

    return False


def csv_colname_to_colindex(csv_file, colnames, expected_colindex, encoding='utf-8-sig'):
    colindex = list()

    firstrow = list()
    with open(csv_file, mode='r', encoding=encoding) as infile:
        reader = csv.reader(infile)
        for rows in reader:
            for colname in rows:
                firstrow.append(colname.strip().upper())
            break

    for colname in colnames:
        try:
            index = firstrow.index(colname.strip().upper())
        except ValueError:
            index = -1
        colindex.append(index)

    t_colindex = tuple(colindex)
    if expected_colindex is not None and len(expected_colindex) > 0:
        if t_colindex != tuple(expected_colindex):
            logging.warning("Index in csv file is not as expected. Please double check.")
            logging.warning("      Columns: %s", colnames)
            logging.warning("      Indices: %s", t_colindex)
            logging.warning("      Expected: %s", tuple(expected_colindex))

    return t_colindex


def read_wellboreidnames_from_csv(csv_file):
    dict_uniqid_boreidname = dict()
    non_uniqids = set()
    name_unique_wellbore_identifier = "Unique Wellbore Identifier"
    name_alias = "Alias"
    name_wellbore_name_set = "Wellbore Name Set"
    name_wellbore_name = "Wellbore Name"
    col_unique_wellbore_identifier = 0
    col_alias = 3
    col_wellbore_name_set = 4
    col_wellbore_name = 2

    (col_unique_wellbore_identifier, col_alias, col_wellbore_name_set, col_wellbore_name) = \
        csv_colname_to_colindex(csv_file,
                                (name_unique_wellbore_identifier, name_alias,
                                 name_wellbore_name_set, name_wellbore_name),
                                (col_unique_wellbore_identifier, col_alias,
                                 col_wellbore_name_set, col_wellbore_name))

    with open(csv_file, mode='r') as infile:
        reader = csv.reader(infile)
        for rows in reader:
            if "SHELL_UWBI" == rows[col_wellbore_name_set].strip():
                uniqid = rows[col_unique_wellbore_identifier].strip()
                wellboreid = rows[col_alias].strip()
                wellborename = rows[col_wellbore_name].strip()
                if uniqid not in non_uniqids:
                    if uniqid in dict_uniqid_boreidname:
                        # duplicate key found, do not trust and use
                        logging.warning("Duplicate wellbore ids found for uniqid: " + uniqid)
                        non_uniqids.add(uniqid)
                        del dict_uniqid_boreidname[uniqid]
                    else:
                        dict_uniqid_boreidname[uniqid] = (wellboreid, wellborename)

    return dict_uniqid_boreidname


def get_wellboreidname_from_uniqid(dict_uniqid_boreidname, uniqid):
    return dict_uniqid_boreidname[uniqid]


def read_document_wellboreids_from_csv(csv_file):
    dict_full_document_boreid = dict()
    dict_document_boreid = dict()
    non_uniqdocnames = set()
    name_unique_wellbore_identifier = 'Unique Wellbore Identifier'
    name_report_file_name = 'Report File Name'
    col_unique_wellbore_identifier = 0
    col_report_file_name = 6

    (col_unique_wellbore_identifier, col_report_file_name) = \
        csv_colname_to_colindex(csv_file,
                                (name_unique_wellbore_identifier, name_report_file_name),
                                (col_unique_wellbore_identifier, col_report_file_name),
                                 encoding=None)

    with open(csv_file, mode='r') as infile:
        reader = csv.reader(infile)
        for rows in reader:
            full_doc_name = rows[col_report_file_name].strip()
            unique_wellboreid = rows[col_unique_wellbore_identifier].strip()

            temp_uniqids = set()
            if full_doc_name in dict_full_document_boreid:
                temp_uniqids = dict_full_document_boreid[full_doc_name]
            else:
                dict_full_document_boreid[full_doc_name] = temp_uniqids
            temp_uniqids.add(unique_wellboreid)

    for full_doc_name, uniqids in dict_full_document_boreid.items():
        short_doc_name = full_doc_name[full_doc_name.rfind("/") + 1:]
        short_doc_name = short_doc_name[short_doc_name.rfind("\\") + 1:]
        unique_wellboreid = set(uniqids)

        if short_doc_name not in non_uniqdocnames:
            if short_doc_name in dict_document_boreid:
                # duplicate doc name found, do not trust if the boreid is different
                if dict_document_boreid[short_doc_name] != unique_wellboreid:
                    logging.warning("Duplicate document names found in wellbore_document.csv : " + short_doc_name)
                    non_uniqdocnames.add(short_doc_name)
                    del dict_document_boreid[short_doc_name]
            else:
                dict_document_boreid[short_doc_name] = unique_wellboreid

    return dict_document_boreid


def get_wellbore_uniqueid_from_docname(dict_document_boreid, doc_name):
    return dict_document_boreid[doc_name]


def get_zipfile_filenamelist(input_path, zip_filename):
    namelist = []

    def add_inner_zipfile_filenamelist(parent_zip, child_zip_path, parent_zip_path, name_list):
        childzip_filename = parent_zip_path + child_zip_path
        childzip_filename_as_folder = childzip_filename[:childzip_filename.rfind(".")].strip() + "/"

        memory_zip = BytesIO()
        memory_zip.write(parent_zip.open(child_zip_path).read())
        memory_zipfile = ZipFile(memory_zip)
        childnames = memory_zipfile.namelist()
        for childname in childnames:
            if not memory_zipfile.getinfo(childname).is_dir():
                if childname.lower().endswith(".zip"):
                    add_inner_zipfile_filenamelist(memory_zipfile, childname, childzip_filename_as_folder, name_list)
                else:
                    namelist.append(childzip_filename_as_folder + childname)

    full_path = os.path.join(input_path, zip_filename)
    zip_filename_as_folder = zip_filename[:zip_filename.rfind(".")].strip()+"/"
    with ZipFile(full_path, 'r') as f_zip:
        names = f_zip.namelist()
        for name in names:
            if not f_zip.getinfo(name).is_dir():
                if name.lower().endswith(".zip"):
                    add_inner_zipfile_filenamelist(f_zip, name, zip_filename_as_folder, namelist)
                else:
                    namelist.append(zip_filename_as_folder + name)

    return namelist


def load_schemas(schema_path, schema_ns_name=None, schema_ns_value=None):
    def list_schema_files(path, a_file_list):
        files = os.listdir(path)
        for file in files:
            full_path = os.path.join(path, file)
            if os.path.isfile(full_path):
                if file.endswith('.json'):
                    a_file_list.append(full_path)
            elif os.path.isdir(full_path):
                list_schema_files(full_path, a_file_list)

    dict_schemas = dict()

    # load all json files
    file_list = []
    list_schema_files(schema_path, file_list)
    for schema_file in file_list:
        with open(schema_file, 'r') as fp:
            a_schema = json.load(fp)
            if schema_ns_name is not None and len(schema_ns_name) > 0:
                a_schema = replace_json_namespace(a_schema, schema_ns_name+":", schema_ns_value+":")
            a_id = a_schema.get('$id')
            if a_id is None:
                a_id = a_schema.get('$ID')
            if a_id is not None:
                dict_schemas[a_id] = a_schema

    # resolve latest version
    dict_latest_key = dict()
    dict_latest_version = dict()
    for key, val in dict_schemas.items():
        # strip the version at the end
        key_parts = key.split('/')
        key_version = None
        if len(key_parts) > 1:
            try:
                key_version = int(key_parts[-1])
            except ValueError:
                pass
        if key_version is not None:
            key_latest_id = '/'.join(key_parts[:-1]) + '/'
            previous_key_version = dict_latest_version.get(key_latest_id, None)
            if previous_key_version is None or key_version > previous_key_version:
                dict_latest_version[key_latest_id] = key_version
                dict_latest_key[key_latest_id] = key
    for latest_key, key in dict_latest_key.items():
        dict_schemas[latest_key] = dict_schemas[key]

    return dict_schemas


def validate_schema(lm, schema_id, dict_schemas):
    if schema_id is None or dict_schemas is None:
        return

    schema = dict_schemas.get(schema_id)
    if schema is None:
        logging.warning("Schema is not found for: %s", schema_id)
        return

    resolver = jsonschema.RefResolver("", schema, store=dict_schemas)
    jsonschema.validate(lm, schema, resolver=resolver)


def replace_json_namespace(json_obj, ns_name, ns_value):
    if ns_name is not None and len(ns_name) > 0:
        json_str = json.dumps(json_obj)
        return json.loads(json_str.replace(ns_name, ns_value))

    return json_obj


def url_quote(url_str):
    return quote(url_str, safe='')
