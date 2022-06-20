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

"""create document load manifest files"""

import os
import fnmatch
import json
import logging
import loading_manifest.common_manifest as cm

SCHEMA_VERSION_1_0_0 = "1.0.0"
SUPPORTED_SCHEMA_VERSIONS = (SCHEMA_VERSION_1_0_0, )


def create_document_manifest(document_name, file_type, preload_file_path, file_source,
                             schema_ns_name, schema_ns_value, acl, legal,
                             schema_version, dict_schemas):
    kind_wp = None
    kind_wpc = None
    kind_file = None
    schema_id_lm = None
    schema_id_wp = None
    schema_id_wpc = None
    schema_id_file = None
    if schema_version == SCHEMA_VERSION_1_0_0:
        kind_wp = cm.WorkProductManifest.KIND_GENERIC_WORK_PRODUCT_1_0_0
        kind_wpc = cm.WorkProductComponentManifest.KIND_DOCUMENT_1_0_0
        kind_file = cm.FileManifest.KIND_ID_GENERIC_DATASET_1_0_0
        schema_id_lm = cm.LoadingManifest.SCHEMA_ID_1_0_0
        schema_id_wp = cm.WorkProductManifest.SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0
        schema_id_wpc = cm.WorkProductComponentManifest.SCHEMA_ID_DOCUMENT_1_0_0
        schema_id_file = cm.FileManifest.SCHEMA_ID_GENERIC_DATASET_1_0_0

    # create the work product manifest
    wp = cm.create_work_product_manifest(
        kind=kind_wp,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=document_name,
        description=cm.WorkProductManifest.DESCRIPTION_DOCUMENT
    )

    # create the work product component manifest
    wpc = cm.create_work_product_component_manifest(
        kind=kind_wpc,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=document_name,
        description=cm.WorkProductComponentManifest.DESCRIPTION_DOCUMENT
    )
    # wpc_data = wpc[cm.WorkProductComponentManifest.TAG_DATA]

    # create the document file manifest
    doc_file_name = document_name + "." + file_type
    # file_type_lower = file_type.lower()
    f_doc = cm.create_file_manifest(
        kind=kind_file,
        schema_format_type=None,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        preload_file_path=(preload_file_path + doc_file_name),
        file_source=("" if file_source is None or len(file_source) == 0
                     else (file_source + doc_file_name)),
        file_name=doc_file_name
    )

    # associate wpc with wp
    cm.associate_work_product_components(wp, [wpc])

    # associate document file with wpc
    cm.associate_files(wpc, [f_doc])

    # create document loading manifest
    document = cm.create_loading_manifest(
        work_product=wp,
        work_product_components=[wpc],
        files=[f_doc],
        schema_id_lm=schema_id_lm,
        schema_id_wp=schema_id_wp,
        schema_id_wpc=schema_id_wpc,
        schema_id_file=schema_id_file,
        schema_ns_name=schema_ns_name,
        schema_ns_value=schema_ns_value,
        dict_schemas=dict_schemas
    )

    return document


def create_document_manifest_from_path(
        input_path, output_path,
        preload_file_path, file_source,
        include_files, exclude_files,
        schema_ns_name, schema_ns_value,
        acl, legal, schema_version, dict_schemas):

    # check supported schema version
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logging.error("Schema version %s is not in the supported list: %s", schema_version,
                      SUPPORTED_SCHEMA_VERSIONS)
        logging.info("Generated 0 document load manifests.")
        return

    # list doc filenames
    files = os.listdir(input_path)
    if include_files is not None and len(include_files) > 0:
        files_included = []
        include_patterns = include_files.split(";")
        for f in files:
            for include_pattern in include_patterns:
                if fnmatch.fnmatch(f, include_pattern):
                    files_included.append(f)
                    break
        files = files_included
    if exclude_files is not None and len(exclude_files) > 0:
        files_excluded = []
        exclude_patterns = exclude_files.split(";")
        for f in files:
            for exclude_pattern in exclude_patterns:
                if fnmatch.fnmatch(f, exclude_pattern):
                    files_excluded.append(f)
                    break
        files = [f for f in files if f not in files_excluded]

    valid_files = []
    logging.info("Checking {} files".format(len(files)))
    for file in sorted(files):
        file_doc = file.strip()
        index = file_doc.rfind(".")
        if 0 < index < len(file_doc)-1:
            # minimum check: file and size
            if cm.is_nonzero_file(input_path, file_doc):
                valid_files.append(file_doc)

    processed_files = []
    logging.info("Processing {} files".format(len(valid_files)))
    file_seq_for_too_long = 1
    for valid_file in valid_files:
        index = valid_file.rfind(".")
        document_file = valid_file[0:index]
        file_type = valid_file[index+1:]
        output_file = os.path.join(output_path,
                                   "load_document_" + schema_version + "_"
                                   + document_file + "_" + file_type + ".json")
        if len(output_file) > 259:
            output_file = os.path.join(output_path,
                                       "load_document_long_document_name_" + schema_version + "_"
                                       + str(file_seq_for_too_long)
                                       + "_" + file_type + ".json")
            file_seq_for_too_long = file_seq_for_too_long + 1
        try:
            with open(output_file, "w") as f:
                json.dump(
                    obj=create_document_manifest(
                        document_name=document_file,
                        file_type=file_type,
                        preload_file_path=preload_file_path,
                        file_source=file_source,
                        schema_ns_name=schema_ns_name,
                        schema_ns_value=schema_ns_value,
                        acl=acl,
                        legal=legal,
                        schema_version=schema_version,
                        dict_schemas=dict_schemas
                    ),
                    fp=f,
                    indent=4
                )
                processed_files.append(document_file)
        except Exception:
            logging.exception("Unable to process document file: {}".format(document_file))
            os.remove(output_file)

    logging.info("Generated {} document load manifests.".format(len(processed_files)))
