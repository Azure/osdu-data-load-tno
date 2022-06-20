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

"""Main module to run from the command line"""

import os
import sys
import getopt
import logging
import json
import pkg_resources

import loading_manifest.common_manifest as cm
import loading_manifest.osdu_laslog_manifest as m_laslog
import loading_manifest.osdu_wellborepath_manifest as m_path
import loading_manifest.osdu_document_manifest as m_document
import loading_manifest.osdu_wellboretop_manifest as m_top


DEFAULT_SCHEMA_VERSION = "1.0.0"


def main(argv):
    manifest_type = ""
    input_path = ""
    output_path = ""
    preload_file_path = ""
    file_source = ""
    include_files = ""
    exclude_files = ""
    osdu_authorid = ""
    schema_path = ""
    schema_ns_name = ""
    schema_ns_value = ""
    acl_owners = "[]"
    acl_viewers = "[]"
    legal_legaltags = "[]"
    legal_countries = "[]"
    schema_version = DEFAULT_SCHEMA_VERSION
    uom_persistable_reference = "{}"

    usage = "usage: python -m loading_manifest.main_swps " \
            "--type <laslog|path|document|top> " \
            "--input_path <inputpath> " \
            "--output_path <outputpath> " \
            "--preload_file_path <preloadfilepath> " \
            "--file_source <filesource> " \
            "--include_files <pattern1|pattern2...> " \
            "--exclude_files <pattern1|pattern2...> " \
            "--curve_quality <curve_quality> " \
            "--schema_path <schemapath> " \
            "--schema_ns_name <namespace_name> " \
            "--schema_ns_value <namespace_value " \
            "--acl_owners <acl_owners> " \
            "--acl_viewers <acl_viewers> " \
            "--legal_legaltags <legal_legaltags> " \
            "--legal_countries <legal_countries> " \
            "--schema_version <schema_version> " \
            "--uom_persistable_reference <uom_persistable_reference>"

    type_las = "laslog"
    type_path = "path"
    type_document = "document"
    type_top = "top"

    types = [type_las, type_path, type_document, type_top]

    try:
        opts, args = getopt.getopt(argv, "h",
                                   ["type=", "input_path=", "output_path=",
                                    "preload_file_path=", "file_source=",
                                    "include_files=", "exclude_files=", "osdu_authorid=",
                                    "schema_path=", "schema_ns_name=", "schema_ns_value=",
                                    "acl_owners=", "acl_viewers=",
                                    "legal_legaltags=", "legal_countries=",
                                    "schema_version=", "uom_persistable_reference="])
    except getopt.GetoptError as e:
        logging.info(usage)
        logging.exception(str(e))
        return

    for opt, arg in opts:
        if opt == '-h':
            print(usage)
            return
        elif opt == "--type":
            manifest_type = arg.strip().lower()
        elif opt == "--input_path":
            input_path = arg.strip()
            if len(input_path) > 0:
                input_path = os.path.abspath(input_path)
        elif opt == "--output_path":
            output_path = arg.strip()
            if len(output_path) > 0:
                output_path = os.path.abspath(output_path)
        elif opt == "--preload_file_path":
            preload_file_path = arg.strip()
        elif opt == "--file_source":
            file_source = arg.strip()
        elif opt == "--include_files":
            include_files = arg.strip()
        elif opt == "--exclude_files":
            exclude_files = arg.strip()
        elif opt == "--osdu_authorid":
            osdu_authorid = arg.strip()
        elif opt == "--schema_path":
            schema_path = arg.strip()
            if len(schema_path) > 0:
                schema_path = os.path.abspath(schema_path)
        elif opt == "--schema_ns_name":
            schema_ns_name = arg.strip()
        elif opt == "--schema_ns_value":
            schema_ns_value = arg.strip()
        elif opt == "--acl_owners":
            if len(arg.strip()) > 0:
                acl_owners = arg.strip()
        elif opt == "--acl_viewers":
            if len(arg.strip()) > 0:
                acl_viewers = arg.strip()
        elif opt == "--legal_legaltags":
            if len(arg.strip()) > 0:
                legal_legaltags = arg.strip()
        elif opt == "--legal_countries":
            if len(arg.strip()) > 0:
                legal_countries = arg.strip()
        elif opt == "--schema_version":
            if len(arg.strip()) > 0:
                schema_version = arg.strip()
        elif opt == "--uom_persistable_reference":
            if len(arg.strip()) > 0:
                uom_persistable_reference = arg.strip()

    use_uom_persistable_reference = (schema_version > DEFAULT_SCHEMA_VERSION
                                     and manifest_type == type_las)
    if use_uom_persistable_reference:
        if uom_persistable_reference is not None and len(uom_persistable_reference) > 0:
            uom_persistable_reference = eval(uom_persistable_reference)
            assert(isinstance(uom_persistable_reference, dict))

        # read and merge with default uom_persistable_reference
        default_uom_persistable_reference = {}
        with open(pkg_resources.resource_filename(__name__, "UOM_Persistable_Reference.json"), 'r', encoding='utf-8') as fp:
            default_uom_persistable_reference = json.load(fp)
        default_uom_persistable_reference.update(uom_persistable_reference)
        uom_persistable_reference = default_uom_persistable_reference


    if manifest_type is not None and len(manifest_type) > 0:
        logging.info("manifest_type: %s", manifest_type)
    if input_path is not None and len(input_path) > 0:
        logging.info("input_path: %s", input_path)
    if output_path is not None and len(output_path) > 0:
        logging.info("output_path: %s", output_path)
    if preload_file_path is not None and len(preload_file_path) > 0:
        logging.info("preload_file_path: %s", preload_file_path)
    if file_source is not None and len(file_source) > 0:
        logging.info("file_source: %s", file_source)
    if include_files is not None and len(include_files) > 0:
        logging.info("include_files: %s", include_files)
    if exclude_files is not None and len(exclude_files) > 0:
        logging.info("exclude_files: %s", exclude_files)
    if osdu_authorid is not None and len(osdu_authorid) > 0:
        logging.info("osdu_authorid: %s", osdu_authorid)
    if schema_path is not None and len(schema_path) > 0:
        logging.info("schema_path: %s", schema_path)
    if schema_ns_name is not None and len(schema_ns_name) > 0:
        logging.info("schema_ns_name: %s", schema_ns_name)
    if schema_ns_value is not None and len(schema_ns_value) > 0:
        logging.info("schema_ns_value: %s", schema_ns_value)
    if acl_owners is not None and len(acl_owners) > 0:
        acl_owners = eval(acl_owners)
        logging.info("acl_owners: %s", str(acl_owners))
    if acl_viewers is not None and len(acl_viewers) > 0:
        acl_viewers = eval(acl_viewers)
        logging.info("acl_viewers: %s", str(acl_viewers))
    if legal_legaltags is not None and len(legal_legaltags) > 0:
        legal_legaltags = eval(legal_legaltags)
        logging.info("legal_legaltags: %s", str(legal_legaltags))
    if legal_countries is not None and len(legal_countries) > 0:
        legal_countries = eval(legal_countries)
        logging.info("legal_countries: %s", str(legal_countries))
    if use_uom_persistable_reference and uom_persistable_reference is not None \
            and len(uom_persistable_reference) > 0:
        logging.info("uom_persistable_reference: %s", json.dumps(uom_persistable_reference, indent=2))
    if schema_version is not None and len(schema_version) > 0:
        logging.info("schema_version: %s", schema_version)

    logging.info("\n")

    if len(input_path) == 0 \
        or len(output_path) == 0 \
            or len(preload_file_path) == 0 \
            or manifest_type not in types:
        logging.error(usage)
        return

    # create output path if it does not exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    preload_file_path = cm.append_file_separator(preload_file_path)
    file_source = cm.append_file_separator(file_source)

    # load schemas if available
    dict_schemas = None
    if schema_path is not None and len(schema_path) > 0:
        dict_schemas = cm.load_schemas(schema_path, schema_ns_name, schema_ns_value)

    acl = {
        "owners": acl_owners,
        "viewers": acl_viewers
    }
    legal = {
        "legaltags": legal_legaltags,
        "otherRelevantDataCountries": legal_countries
    }

    if manifest_type == type_las:
        m_laslog.create_laslog_manifest_from_path(
            input_path=input_path,
            output_path=output_path,
            preload_file_path=preload_file_path,
            file_source=file_source,
            osdu_authorid=osdu_authorid,
            schema_ns_name=schema_ns_name,
            schema_ns_value=schema_ns_value,
            acl=acl,
            legal=legal,
            uom_persistable_reference=uom_persistable_reference,
            schema_version=schema_version,
            dict_schemas=dict_schemas)
    elif manifest_type == type_path:
        m_path.create_wellborepath_manifest_from_path(
            input_path=input_path,
            output_path=output_path,
            preload_file_path=preload_file_path,
            file_source=file_source,
            schema_ns_name=schema_ns_name,
            schema_ns_value=schema_ns_value,
            acl=acl,
            legal=legal,
            schema_version=schema_version,
            dict_schemas=dict_schemas)
    elif manifest_type == type_document:
        m_document.create_document_manifest_from_path(
            input_path=input_path,
            output_path=output_path,
            preload_file_path=preload_file_path,
            file_source=file_source,
            include_files=include_files,
            exclude_files=exclude_files,
            schema_ns_name=schema_ns_name,
            schema_ns_value=schema_ns_value,
            acl=acl,
            legal=legal,
            schema_version=schema_version,
            dict_schemas=dict_schemas)
    elif manifest_type == type_top:
        m_top.create_wellboretop_manifest_from_path(
            input_path=input_path,
            output_path=output_path,
            preload_file_path=preload_file_path,
            file_source=file_source,
            schema_ns_name=schema_ns_name,
            schema_ns_value=schema_ns_value,
            acl=acl,
            legal=legal,
            schema_version=schema_version,
            dict_schemas=dict_schemas)


if __name__ == "__main__":
    import coloredlogs

    FIELD_STYLES = dict(
        asctime=dict(color='green'),
        hostname=dict(color='magenta'),
        levelname=dict(color='green', bold=coloredlogs.CAN_USE_BOLD_FONT),
        filename=dict(color='magenta'),
        name=dict(color='blue'),
        threadName=dict(color='green')
    )
    coloredlogs.install(
        level='INFO',
        fmt='%(asctime)s %(levelname)s %(message)s',
        field_styles=FIELD_STYLES
    )

    main(sys.argv[1:])
