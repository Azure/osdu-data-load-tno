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

"""create wellboretop load manifest files"""

import os
import fnmatch
import json
import csv
import logging
import loading_manifest.common_manifest as cm


SCHEMA_VERSION_1_0_0 = "1.0.0"
SUPPORTED_SCHEMA_VERSIONS = (SCHEMA_VERSION_1_0_0,)


def read_markers_from_csv_file(csv_file):
    marker_name_types = []
    name_top_md = "TOP_MD"
    name_strat_unit_cd = "STRAT_UNIT_CD"
    name_strat_unit_nm = "STRAT_UNIT_NM"
    name_depth_unit = "DEPTH_UNITS"
    col_top_md = 2
    col_strat_unit_cd = 6
    col_strat_unit_nm = 7
    col_depth_unit = 15

    (col_top_md, col_strat_unit_cd, col_strat_unit_nm, col_depth_unit) = \
        cm.csv_colname_to_colindex(csv_file,
                                   (name_top_md, name_strat_unit_cd, name_strat_unit_nm, name_depth_unit),
                                   (col_top_md, col_strat_unit_cd, col_strat_unit_nm, col_depth_unit))

    with open(csv_file, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        skip_first_row = True
        for rows in reader:
            if skip_first_row:
                skip_first_row = False
                continue
            marker_top_md = float(rows[col_top_md].strip())
            marker_cd = rows[col_strat_unit_cd].strip()
            marker_nm = rows[col_strat_unit_nm].strip()
            marker_depth_unit = rows[col_depth_unit].strip().lower()
            marker_name_types.append((marker_top_md, marker_depth_unit, marker_nm, marker_cd))

    return marker_name_types


def create_wellboretop_manifest(wellboretop_name, preload_file_path, file_source,
                                wellbore_id, top_markers,
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
        kind_wpc = cm.WorkProductComponentManifest.KIND_WELLBORE_TOP_1_0_0
        kind_file = cm.FileManifest.KIND_ID_GENERIC_DATASET_1_0_0
        schema_id_lm = cm.LoadingManifest.SCHEMA_ID_1_0_0
        schema_id_wp = cm.WorkProductManifest.SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0
        schema_id_wpc = cm.WorkProductComponentManifest.SCHEMA_ID_WELLBORE_TOP_1_0_0
        schema_id_file = cm.FileManifest.SCHEMA_ID_GENERIC_DATASET_1_0_0

    # create the work product manifest
    wp = cm.create_work_product_manifest(
        kind=kind_wp,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=wellboretop_name,
        description=cm.WorkProductManifest.DESCRIPTION_WELLBORE_TOP
    )

    # create the work product component manifest
    wpc = cm.create_work_product_component_manifest(
        kind=kind_wpc,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=wellboretop_name,
        description=cm.WorkProductComponentManifest.DESCRIPTION_WELLBORE_TOP
    )
    wpc_data = wpc[cm.WorkProductComponentManifest.TAG_DATA]
    wpc_data[cm.WorkProductComponentManifest.TAG_DATA_WELL_BORE_ID] \
        = cm.WorkProductComponentManifest.WELL_BORE_ID + cm.url_quote(wellbore_id) + ":"
    wpc_top_markers = []
    marker_depth_unit_1 = None
    for marker_top_md, marker_depth_unit, marker_name, marker_type in top_markers:
        if marker_depth_unit_1 is None:
            marker_depth_unit_1 = marker_depth_unit
        elif marker_depth_unit_1 != marker_depth_unit:
            logging.warning("Inconsistent depth units for %s: %s, %s", wellboretop_name,
                            marker_depth_unit_1, marker_depth_unit)
        marker_item = dict()
        marker_item[
            cm.WorkProductComponentManifest.TAG_DATA_MERKERS_MARKER_NAME] = marker_name
        marker_item[
            cm.WorkProductComponentManifest.TAG_DATA_MERKERS_MARKER_DEPTH] = marker_top_md
        wpc_top_markers.append(marker_item)
    # if marker_depth_unit_1 is not None:
        # wpc_data[cm.WorkProductComponentManifest.TAG_DATA_DEPTH_UNIT] \
        #     = cm.WorkProductComponentManifest.UOM_ID + marker_depth_unit_1 + ":"
    wpc_data[cm.WorkProductComponentManifest.TAG_DATA_MERKERS] = wpc_top_markers

    wpc_meta = wpc[cm.WorkProductComponentManifest.TAG_META]
    if marker_depth_unit_1 is not None and len(marker_depth_unit_1) > 0:
        meta_unit = dict()
        wpc_meta.append(meta_unit)
        meta_unit[cm.WorkProductComponentManifest.TAG_META_KIND] = cm.WorkProductComponentManifest.META_KIND_UNIT
        meta_unit[cm.WorkProductComponentManifest.TAG_META_NAME] = marker_depth_unit_1
        meta_unit[cm.WorkProductComponentManifest.TAG_META_PERSIST_REF] = ""
        meta_unit[cm.WorkProductComponentManifest.TAG_META_UOM] = \
            cm.WorkProductComponentManifest.UOM_ID + marker_depth_unit_1 + ":"
        meta_unit[cm.WorkProductComponentManifest.TAG_META_PROPERTY_NAMES] = []
        meta_unit[cm.WorkProductComponentManifest.TAG_META_PROPERTY_NAMES].append("Markers[].MarkerMeasuredDepth")

    # create the wellboretop file manifest
    wellboretop_file_name = wellboretop_name
    f_doc = cm.create_file_manifest(
        kind=kind_file,
        schema_format_type=cm.FileManifest.SCHEMA_FORMAT_TYPE_ID_CVS,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        preload_file_path=(preload_file_path + wellboretop_file_name),
        file_source=("" if file_source is None or len(file_source) == 0
                     else (file_source + wellboretop_file_name)),
        file_name=wellboretop_file_name
    )

    # associate wpc with wp
    cm.associate_work_product_components(wp, [wpc])

    # associate wellboretop file with wpc
    cm.associate_files(wpc, [f_doc])

    # create wellboretop loading manifest
    wellboretop = cm.create_loading_manifest(
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

    return wellboretop


def create_wellboretop_manifest_from_path(
        input_path, output_path,
        preload_file_path, file_source,
        schema_ns_name, schema_ns_value,
        acl, legal, schema_version, dict_schemas):

    # check supported schema version
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logging.error("Schema version %s is not in the supported list: %s", schema_version,
                      SUPPORTED_SCHEMA_VERSIONS)
        logging.info("Generated 0 wellboretop load manifests.")
        return

    # list top filenames
    files = os.listdir(input_path)
    files_wellboretop = fnmatch.filter(files, "*.csv")

    valid_files = []
    logging.info("Checking {} files".format(len(files_wellboretop)))
    for file_wellboretop in sorted(files_wellboretop):
        file_wellboretop = file_wellboretop.strip()
        # minimum check: file and size
        if cm.is_nonzero_file(input_path, file_wellboretop):
            valid_files.append(file_wellboretop)

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
        #     logging.warning("Skip Invalid File:" + valid_file)
        #     continue

        wellboretop_file = valid_file
        output_file = os.path.join(output_path,
                                   "load_top_" + schema_version + "_"
                                   + wellboretop_file.replace(".", "_") + ".json")
        try:
            # retrieve markers
            top_markers = read_markers_from_csv_file(full_valid_file_path)
            if top_markers is None or len(top_markers) == 0:
                logging.warning("Markers not found for: " + full_valid_file_path)

            with open(output_file, "w") as f:
                json.dump(
                    obj=create_wellboretop_manifest(
                        wellboretop_name=wellboretop_file,
                        preload_file_path=preload_file_path,
                        file_source=file_source,
                        wellbore_id=wellbore_id,
                        top_markers=top_markers,
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
                processed_files.append(wellboretop_file)
        except Exception:
            logging.exception("Unable to process wellboretop file: {}".format(wellboretop_file))
            os.remove(output_file)

    logging.info("Generated {} wellboretop load manifests.".format(len(processed_files)))
