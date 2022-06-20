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

"""create wellborepath load manifest files"""

import os
import fnmatch
import json
import csv
import logging
import loading_manifest.common_manifest as cm


SCHEMA_VERSION_1_0_0 = "1.0.0"
SCHEMA_VERSION_1_1_0 = "1.1.0"
SUPPORTED_SCHEMA_VERSIONS = (SCHEMA_VERSION_1_0_0, SCHEMA_VERSION_1_1_0)


def read_data_from_csv_file(csv_file):
    top_md = None
    base_md = None
    name_md = "MD"
    col_md = 0

    (col_md,) = \
        cm.csv_colname_to_colindex(csv_file,
                                   (name_md,),
                                   (col_md,))

    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        skip_first_row = True
        for rows in reader:
            if skip_first_row:
                skip_first_row = False
                continue
            md = float(rows[col_md].strip())
            if top_md is None or md < top_md:
                top_md = md
            if base_md is None or md > base_md:
                base_md = md

    return top_md, base_md


def create_wellborepath_manifest(wellborepath_name, preload_file_path, file_source,
                                 wellbore_id, top_md, base_md,
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
        kind_wpc = cm.WorkProductComponentManifest.KIND_WELLBORE_PATH_1_0_0
        kind_file = cm.FileManifest.KIND_ID_GENERIC_DATASET_1_0_0
        schema_id_lm = cm.LoadingManifest.SCHEMA_ID_1_0_0
        schema_id_wp = cm.WorkProductManifest.SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0
        schema_id_wpc = cm.WorkProductComponentManifest.SCHEMA_ID_WELLBORE_PATH_1_0_0
        schema_id_file = cm.FileManifest.SCHEMA_ID_GENERIC_DATASET_1_0_0
    elif schema_version == SCHEMA_VERSION_1_1_0:
        kind_wp = cm.WorkProductManifest.KIND_GENERIC_WORK_PRODUCT_1_0_0
        kind_wpc = cm.WorkProductComponentManifest.KIND_WELLBORE_PATH_1_1_0
        kind_file = cm.FileManifest.KIND_ID_GENERIC_DATASET_1_0_0
        schema_id_lm = cm.LoadingManifest.SCHEMA_ID_1_0_0
        schema_id_wp = cm.WorkProductManifest.SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0
        schema_id_wpc = cm.WorkProductComponentManifest.SCHEMA_ID_WELLBORE_PATH_1_1_0
        schema_id_file = cm.FileManifest.SCHEMA_ID_GENERIC_DATASET_1_0_0

    # create the work product manifest
    wp = cm.create_work_product_manifest(
        kind=kind_wp,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=wellborepath_name,
        description=cm.WorkProductManifest.DESCRIPTION_WELLBORE_PATH
    )

    # create the work product component manifest
    wpc = cm.create_work_product_component_manifest(
        kind=kind_wpc,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=wellborepath_name,
        description=cm.WorkProductComponentManifest.DESCRIPTION_WELLBORE_PATH
    )
    wpc_data = wpc[cm.WorkProductComponentManifest.TAG_DATA]
    wpc_data[cm.WorkProductComponentManifest.TAG_DATA_WELL_BORE_ID] \
        = cm.WorkProductComponentManifest.WELL_BORE_ID + cm.url_quote(wellbore_id) + ":"
    if top_md is not None:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_TOP_MD] = top_md
    if base_md is not None:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_BASE_MD] = base_md
    wpc_data[cm.WorkProductComponentManifest.TAG_DATA_VERTICAL_MEASUREMENT] = {}

    # create the wellborepath file manifest
    wellborepath_file_name = wellborepath_name
    f_doc = cm.create_file_manifest(
        kind=kind_file,
        schema_format_type=cm.FileManifest.SCHEMA_FORMAT_TYPE_ID_CVS,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        preload_file_path=(preload_file_path + wellborepath_file_name),
        file_source=("" if file_source is None or len(file_source) == 0
                     else (file_source + wellborepath_file_name)),
        file_name=wellborepath_file_name
    )

    # associate wpc with wp
    cm.associate_work_product_components(wp, [wpc])

    # associate wellborepath file with wpc
    cm.associate_files(wpc, [f_doc])

    # create wellborepath loading manifest
    wellborepath = cm.create_loading_manifest(
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

    return wellborepath


def create_wellborepath_manifest_from_path(
        input_path, output_path,
        preload_file_path, file_source,
        schema_ns_name, schema_ns_value,
        acl, legal, schema_version, dict_schemas):

    # check supported schema version
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logging.error("Schema version %s is not in the supported list: %s", schema_version,
                      SUPPORTED_SCHEMA_VERSIONS)
        logging.info("Generated 0 trajectory load manifests.")
        return

    # list path filenames
    files = os.listdir(input_path)
    files_wellborepath = fnmatch.filter(files, "*.csv")

    valid_files = []
    logging.info("Checking {} files".format(len(files_wellborepath)))
    for file_wellborepath in sorted(files_wellborepath):
        file_wellborepath = file_wellborepath.strip()
        # minimum check: file and size
        if cm.is_nonzero_file(input_path, file_wellborepath):
            valid_files.append(file_wellborepath)

    processed_files = []
    logging.info("Processing {} files".format(len(valid_files)))
    for valid_file in valid_files:
        full_valid_file_path = os.path.join(input_path, valid_file)

        # retrieve wellbore id
        # osdu
        wellbore_id = valid_file[:-4]
        # try:
        #     int(wellbore_id)
        # except ValueError:
        #     logging.warning("Skip " + valid_file)
        #     continue

        wellborepath_file = valid_file
        output_file = os.path.join(output_path,
                                   "load_path_" + schema_version + "_"
                                   + wellborepath_file.replace(".", "_") + ".json")
        try:
            # retrieve top/base mds from csv file
            top_md, base_md = read_data_from_csv_file(full_valid_file_path)

            with open(output_file, "w") as f:
                json.dump(
                    obj=create_wellborepath_manifest(
                        wellborepath_name=wellborepath_file,
                        preload_file_path=preload_file_path,
                        file_source=file_source,
                        wellbore_id=wellbore_id,
                        top_md=top_md,
                        base_md=base_md,
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
                processed_files.append(wellborepath_file)
        except Exception:
            logging.exception("Unable to process wellborepath file: {}".format(wellborepath_file))
            os.remove(output_file)

    logging.info("Generated {} trajectory load manifests.".format(len(processed_files)))
