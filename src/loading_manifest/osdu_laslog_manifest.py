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

"""create laslog load manifest files"""

import os
import fnmatch
import json
import lasio
import math
import logging
import loading_manifest.common_manifest as cm


SCHEMA_VERSION_1_0_0 = "1.0.0"
SCHEMA_VERSION_1_1_0 = "1.1.0"
SUPPORTED_SCHEMA_VERSIONS = (SCHEMA_VERSION_1_0_0, SCHEMA_VERSION_1_1_0)


def read_data_from_log_file(log_file):

    def item_strvalue(section, field_name, check_float=False):
        try:
            val = str(section[field_name].value).strip()
            if check_float:
                try:
                    float(val)
                except ValueError:
                    logging.warning("Non-number depth: " + val + "  in log file: " + log_file)
                    val = None
            return val
        except Exception:
            return None

    def item_unit(section, field_name):
        try:
            return str(section[field_name].unit).strip()
        except Exception:
            return None

    with open(log_file, "r") as fp:
        las = lasio.read(fp)

        # version
        las_version = item_strvalue(las.version, 'VERS')

        # well section
        # osdu log_uniqid = item_strvalue(las.well, 'UWBI')
        log_uniqid = item_strvalue(las.params, 'UBID')
        if log_uniqid is None or len(log_uniqid) == 0:
            log_uniqid = item_strvalue(las.well, 'UWI')
        if log_uniqid is None or len(log_uniqid) == 0:
            log_uniqid = item_strvalue(las.well, 'WELL')
        well_name = item_strvalue(las.well, 'WELL')
        logging_contractor = item_strvalue(las.well, 'SRVC')
        date_logged = item_strvalue(las.well, 'DATE')
        log_depth_units = item_unit(las.well, 'STRT')
        log_start_depth = item_strvalue(las.well, 'STRT', True)
        log_stop_depth = item_strvalue(las.well, 'STOP', True)
        log_depth_step = item_strvalue(las.well, 'STEP', True)
        log_null_value = item_strvalue(las.well, 'NULL', True)

        # parameters section
        log_name = item_strvalue(las.params, 'LNAM')
        log_type = item_strvalue(las.params, 'LTYP')
        log_source = item_strvalue(las.params, 'LSOU')
        log_activity = item_strvalue(las.params, 'LACT')
        log_version = item_strvalue(las.params, 'LVSN')

        # top/bottom depths
        log_top_depth = log_start_depth
        log_bottom_depth = log_stop_depth
        if (log_start_depth is not None and log_stop_depth is not None
                and float(log_start_depth) > float(log_stop_depth)):
            log_top_depth = log_stop_depth
            log_bottom_depth = log_start_depth

        # curve section
        log_curves = []
        # 'mnemonic' = curves[].mnemonic
        # 'curve_type' =  seems not in the las file ????
        # 'curve_unit' =  curves[].unit
        # 'curve_top_depth' =  remove top/bottom null values
        # 'curve_bottom_depth' =  remove top/bottom null values
        if len(las.curves) > 0:  # > 1: for osdu, not to skip the first channel
            # skip first channel
            # for osdu, not to skip the first channel
            for curveitem in las.curves:
                log_curve = dict()
                log_curves.append(log_curve)
                log_curve['mnemonic'] = curveitem.mnemonic
                log_curve['unit'] = curveitem.unit
                log_curve['depth_units'] = log_depth_units
                # derive top/bottom depths for each curve -- stripping off the start/end null values
                curve_start_depth = None
                curve_stop_depth = None
                for i in range(len(curveitem.data)):
                    if not math.isnan(curveitem.data[i]):
                        curve_start_depth = las.curves[0].data[i]
                        break
                for i in reversed(range(len(curveitem.data))):
                    if not math.isnan(curveitem.data[i]):
                        curve_stop_depth = las.curves[0].data[i]
                        break

                if curve_start_depth is None:
                    log_curve['top_depth'] = None
                    log_curve['bottom_depth'] = None
                else:
                    log_curve['top_depth'] = '{:f}'.format(curve_start_depth).rstrip('0').rstrip('.')
                    log_curve['bottom_depth'] = '{:f}'.format(curve_stop_depth).rstrip('0').rstrip('.')
                if (curve_start_depth is not None and curve_stop_depth is not None
                        and float(curve_start_depth) > float(curve_stop_depth)):
                    log_curve['top_depth'] = '{:f}'.format(curve_stop_depth).rstrip('0').rstrip('.')
                    log_curve['bottom_depth'] = '{:f}'.format(curve_start_depth).rstrip('0').rstrip('.')

    # return as a dict
    return {
        'las_version': las_version,
        'log_uniqid': log_uniqid,
        'well_name': well_name,
        'log_name': log_name,
        'log_type': log_type,
        'log_source': log_source,
        'log_activity': log_activity,
        'log_version': log_version,
        'logging_contractor': logging_contractor,
        'date_logged': date_logged,
        'log_depth_units': log_depth_units,
        'log_start_depth': log_start_depth,
        'log_stop_depth': log_stop_depth,
        'log_depth_step': log_depth_step,
        'log_top_depth': log_top_depth,
        'log_bottom_depth': log_bottom_depth,
        'log_null_value': log_null_value,
        'log_curves': log_curves
    }


def create_laslog_manifest(laslog_name, preload_file_path, file_source, wellbore_id,
                           log_data, osdu_authorid,
                           schema_ns_name, schema_ns_value,
                           acl, legal, uom_persistable_reference,
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
        kind_wpc = cm.WorkProductComponentManifest.KIND_WELL_LOG_1_0_0
        kind_file = cm.FileManifest.KIND_ID_GENERIC_DATASET_1_0_0
        schema_id_lm = cm.LoadingManifest.SCHEMA_ID_1_0_0
        schema_id_wp = cm.WorkProductManifest.SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0
        schema_id_wpc = cm.WorkProductComponentManifest.SCHEMA_ID_WELL_LOG_1_0_0
        schema_id_file = cm.FileManifest.SCHEMA_ID_GENERIC_DATASET_1_0_0
    elif schema_version == SCHEMA_VERSION_1_1_0:
        kind_wp = cm.WorkProductManifest.KIND_GENERIC_WORK_PRODUCT_1_0_0
        kind_wpc = cm.WorkProductComponentManifest.KIND_WELL_LOG_1_1_0
        kind_file = cm.FileManifest.KIND_ID_GENERIC_DATASET_1_0_0
        schema_id_lm = cm.LoadingManifest.SCHEMA_ID_1_0_0
        schema_id_wp = cm.WorkProductManifest.SCHEMA_ID_GENERIC_WORK_PRODUCT_1_0_0
        schema_id_wpc = cm.WorkProductComponentManifest.SCHEMA_ID_WELL_LOG_1_1_0
        schema_id_file = cm.FileManifest.SCHEMA_ID_GENERIC_DATASET_1_0_0

    # las_version = log_data.get('las_version', None)
    # uniq_id = log_data.get('log_uniqid', None)
    log_curves = log_data.get('log_curves', None)
    log_quality_flag = log_data.get('log_name', None)
    log_type = log_data.get('log_type', None)
    log_source = log_data.get('log_source', None)
    log_activity = log_data.get('log_activity', None)
    log_version = log_data.get('log_version', None)
    log_acquisition_vendor = log_data.get('logging_contractor', None)
    log_depth_units = log_data.get('log_depth_units', None)
    log_start_depth = log_data.get('log_start_depth', None)
    log_stop_depth = log_data.get('log_stop_depth', None)
    log_depth_step = log_data.get('log_depth_step', None)
    log_top_depth = log_data.get('log_top_depth', None)
    log_bottom_depth = log_data.get('log_bottom_depth', None)

    # osdu (log_id, logging_tool) = log_info[log_quality_flag + '&' + uniq_id]
    # log_id = None
    logging_tool = None

    # osdu wp_wpc_name = wellbore_name + " " + log_quality_flag + " LOG"
    # wp_wpc_name = wellbore_name + " LOG"

    # create the work product manifest
    wp = cm.create_work_product_manifest(
        kind=kind_wp,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=laslog_name,
        description=cm.WorkProductManifest.DESCRIPTION_WELL_LOG
    )

    wpc = cm.create_work_product_component_manifest(
        kind=kind_wpc,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        name=laslog_name,
        description=cm.WorkProductComponentManifest.DESCRIPTION_WELL_LOG
    )
    wpc_data = wpc[cm.WorkProductComponentManifest.TAG_DATA]
    if wellbore_id is None:  # osdu
        wellbore_id = ""
    wpc_data[cm.WorkProductComponentManifest.TAG_DATA_WELL_BORE_ID] \
        = cm.WorkProductComponentManifest.WELL_BORE_ID + cm.url_quote(wellbore_id) + ":"
    if log_type is not None and len(log_type) > 0:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_WELL_LOG_TYPE_ID] \
            = cm.WorkProductComponentManifest.WELL_LOG_TYPE_ID + cm.url_quote(log_type) + ":"
    if log_acquisition_vendor is not None and len(log_acquisition_vendor) > 0:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_SERVICE_COMPANY_ID] \
            = cm.WorkProductComponentManifest.SERVICE_COMPANY_ID + cm.url_quote(log_acquisition_vendor) + ":"
    if logging_tool is not None and len(logging_tool) > 0:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_TOOL_DESC] = logging_tool
    if log_source is not None and len(log_source) > 0:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_LOG_SOURCE] = log_source
    if log_activity is not None and len(log_activity) > 0:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_LOG_ACTIVITY] = log_activity
    if log_version is not None and len(log_version) > 0:
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_LOG_VERSION] = log_version
    if log_top_depth is not None and len(log_top_depth) > 0:
        # topd = dict()
        # topd[cm.WorkProductComponentManifest.TAG_DATA_TOP_DEPTH_DEPTH] = float(log_top_depth)
        # topd[cm.WorkProductComponentManifest.TAG_DATA_TOP_DEPTH_UOM] \
        #     = cm.WorkProductComponentManifest.UOM_ID + log_depth_units + ":"
        # wpc_data[cm.WorkProductComponentManifest.TAG_DATA_TOP_DEPTH] = topd
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_TOP_DEPTH] = float(log_top_depth)
    if log_bottom_depth is not None and len(log_bottom_depth) > 0:
        # bottomd = dict()
        # bottomd[cm.WorkProductComponentManifest.TAG_DATA_BOTTOM_DEPTH_DEPTH] \
        #     = float(log_bottom_depth)
        # bottomd[cm.WorkProductComponentManifest.TAG_DATA_BOTTOM_DEPTH_UOM] \
        #     = cm.WorkProductComponentManifest.UOM_ID + log_depth_units + ":"
        # wpc_data[cm.WorkProductComponentManifest.TAG_DATA_BOTTOM_DEPTH] = bottomd
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_BOTTOM_DEPTH] = float(log_bottom_depth)
    if osdu_authorid is not None and len(osdu_authorid) > 0:
        author_ids = list()
        author_ids.append(osdu_authorid)
        wpc_data[cm.WorkProductComponentManifest.TAG_DATA_AUTHOR_IDS] = author_ids

    wpc_log_curves = []
    for log_curve in log_curves:
        curve_item = dict()
        mnemonic = log_curve.get('mnemonic', None)
        top_depth = log_curve.get('top_depth', None)
        bottom_depth = log_curve.get('bottom_depth', None)
        depth_units = log_curve.get('depth_units', None)
        curve_unit = log_curve.get('unit', None)

        # osdu key_id = log_quality_flag + '&' + uniq_id + '&' + log_id + '&' + mnemonic
        # osdu (curve_type, interpreter_name, business_value) = curve_info[key_id]
        curve_type = None
        interpreter_name = None
        business_value = None

        curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_MNEMONIC] = mnemonic
        if curve_type is not None and len(curve_type) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_LOG_CURVE_TYPE_ID] \
                = cm.WorkProductComponentManifest.LOG_CURVE_TYPE_ID + cm.url_quote(curve_type) + ":"
        if business_value is not None and len(business_value) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_LOG_CURVE_BV_ID] \
                = cm.WorkProductComponentManifest.LOG_CURVE_BV_ID + cm.url_quote(business_value) + ":"
        if log_quality_flag is not None and len(log_quality_flag) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_CURVE_QUALITY] \
                = log_quality_flag
        if interpreter_name is not None and len(interpreter_name) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_INTERPRETER_NAME] \
                = interpreter_name
        if top_depth is not None and len(top_depth) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_TOP_DEPTH] \
                = float(top_depth)
        if bottom_depth is not None and len(bottom_depth) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_BASE_DEPTH] \
                = float(bottom_depth)
        if schema_version == SCHEMA_VERSION_1_0_0:
            if depth_units is not None and len(depth_units) > 0:
                curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_DEPTH_UNIT] \
                    = cm.WorkProductComponentManifest.UOM_ID + cm.url_quote(depth_units) + ":"
        if curve_unit is not None and len(curve_unit) > 0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_CURVE_UNIT] \
                = cm.WorkProductComponentManifest.UOM_ID + cm.url_quote(curve_unit) + ":"

        if schema_version >= SCHEMA_VERSION_1_1_0:
            curve_item[cm.WorkProductComponentManifest.TAG_DATA_CURVES_NUMBER_OF_COLUMNS] = 1

        wpc_log_curves.append(curve_item)
    wpc_data[cm.WorkProductComponentManifest.TAG_DATA_CURVES] = wpc_log_curves

    if schema_version >= SCHEMA_VERSION_1_1_0:
        if log_depth_step is not None and len(log_depth_step) > 0:
            wpc_data[cm.WorkProductComponentManifest.TAG_DATA_SAMPLING_INTERVAL] = float(log_depth_step)
        if log_start_depth is not None and len(log_start_depth) > 0:
            wpc_data[cm.WorkProductComponentManifest.TAG_DATA_SAMPLING_START] = float(log_start_depth)
        if log_stop_depth is not None and len(log_stop_depth) > 0:
            wpc_data[cm.WorkProductComponentManifest.TAG_DATA_SAMPLING_STOP] = float(log_stop_depth)

        wpc_meta = wpc[cm.WorkProductComponentManifest.TAG_META]
        if log_depth_units is not None and len(log_depth_units) > 0:
            depth_unit_name = log_depth_units
            depth_unit_pr = ""
            for uom_key in uom_persistable_reference:
                if uom_key.lower() == log_depth_units.lower():
                    depth_unit_name = uom_persistable_reference[uom_key].get('Name', depth_unit_name)
                    depth_unit_pr = uom_persistable_reference[uom_key].get('PersistableReference', "")
                    break
            if depth_unit_pr == "":
                logging.warning("PersistableReference not found for UOM: %s", log_depth_units)
            meta_unit = dict()
            wpc_meta.append(meta_unit)
            meta_unit[cm.WorkProductComponentManifest.TAG_META_KIND] = cm.WorkProductComponentManifest.META_KIND_UNIT
            meta_unit[cm.WorkProductComponentManifest.TAG_META_NAME] = depth_unit_name
            meta_unit[cm.WorkProductComponentManifest.TAG_META_PERSIST_REF] = depth_unit_pr
            meta_unit[cm.WorkProductComponentManifest.TAG_META_UOM] = \
                cm.WorkProductComponentManifest.UOM_ID + cm.url_quote(log_depth_units) + ":"
            meta_unit[cm.WorkProductComponentManifest.TAG_META_PROPERTY_NAMES] = []
            meta_unit[cm.WorkProductComponentManifest.TAG_META_PROPERTY_NAMES].extend(
                [
                    cm.WorkProductComponentManifest.TAG_DATA_TOP_DEPTH,
                    cm.WorkProductComponentManifest.TAG_DATA_BOTTOM_DEPTH,
                    cm.WorkProductComponentManifest.TAG_DATA_SAMPLING_INTERVAL,
                    cm.WorkProductComponentManifest.TAG_DATA_SAMPLING_START,
                    cm.WorkProductComponentManifest.TAG_DATA_SAMPLING_STOP,
                    cm.WorkProductComponentManifest.TAG_DATA_CURVES + "[]."
                    + cm.WorkProductComponentManifest.TAG_DATA_CURVES_TOP_DEPTH,
                    cm.WorkProductComponentManifest.TAG_DATA_CURVES + "[]."
                    + cm.WorkProductComponentManifest.TAG_DATA_CURVES_BASE_DEPTH
                ]
            )

    # create the laslog file manifest
    laslog_file_name = laslog_name
    f_doc = cm.create_file_manifest(
        kind=kind_file,
        schema_format_type=cm.FileManifest.SCHEMA_FORMAT_TYPE_ID_LAS2,
        acl=acl,
        legal=legal,
        resource_security_classificaton=cm.ResourceSecurityClassification.RESTRICTED,
        preload_file_path=(preload_file_path + laslog_file_name),
        file_source=("" if file_source is None or len(file_source) == 0
                     else (file_source + laslog_file_name)),
        file_name=laslog_file_name
    )

    # associate wpc with wp
    cm.associate_work_product_components(wp, [wpc])

    # associate laslog file with wpc
    cm.associate_files(wpc, [f_doc])

    # create laslog loading manifest
    laslog = cm.create_loading_manifest(
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

    return laslog


def create_laslog_manifest_from_path(
        input_path, output_path,
        preload_file_path, file_source,
        osdu_authorid, schema_ns_name, schema_ns_value,
        acl, legal, uom_persistable_reference,
        schema_version, dict_schemas):

    # check supported schema version
    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        logging.error("Schema version %s is not in the supported list: %s", schema_version,
                      SUPPORTED_SCHEMA_VERSIONS)
        logging.info("Generated 0 laslog load manifests.")
        return

    # list laslog filenames
    files = os.listdir(input_path)
    files_laslog = fnmatch.filter(files, "*.las")

    valid_files = []
    logging.info("Checking {} files".format(len(files_laslog)))
    for file_laslog in sorted(files_laslog):
        file_laslog = file_laslog.strip()
        # minimum check: file and size
        if cm.is_nonzero_file(input_path, file_laslog):
            valid_files.append(file_laslog)

    processed_files = []
    logging.info("Processing {} files".format(len(valid_files)))
    for valid_file in valid_files:
        full_valid_file_path = os.path.join(input_path, valid_file)

        # retrieve wellbore id/curves
        try:
            log_data = read_data_from_log_file(full_valid_file_path)
        except Exception:
            logging.exception("Unable to read laslog file: {}".format(full_valid_file_path))
            continue

        uniq_id = log_data.get('log_uniqid', None)
        wellbore_id = uniq_id
        # wellbore_name = log_data.get('well_name', None)
        log_curves = log_data.get('log_curves', None)
        if log_curves is None or len(log_curves) <= 0:
            logging.warning("LogCurves not found for: " + full_valid_file_path)

        laslog_file = valid_file
        output_file = os.path.join(output_path,
                                   "load_log_" + schema_version + "_"
                                   + laslog_file.replace(".", "_") + ".json")
        try:
            with open(output_file, "w") as f:
                json.dump(
                    obj=create_laslog_manifest(
                        laslog_name=laslog_file,
                        preload_file_path=preload_file_path,
                        file_source=file_source,
                        wellbore_id=wellbore_id,
                        log_data=log_data,
                        osdu_authorid=osdu_authorid,
                        schema_ns_name=schema_ns_name,
                        schema_ns_value=schema_ns_value,
                        acl=acl,
                        legal=legal,
                        uom_persistable_reference=uom_persistable_reference,
                        schema_version=schema_version,
                        dict_schemas=dict_schemas
                    ),
                    fp=f,
                    indent=4
                )
                processed_files.append(laslog_file)
        except Exception:
            logging.exception("Unable to process laslog file: {}".format(laslog_file))
            os.remove(output_file)

    logging.info("Generated {} laslog load manifests.".format(len(processed_files)))
