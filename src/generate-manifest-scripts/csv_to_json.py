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

import os
import json
import csv
import re
import copy
import datetime
import logging

parameter_start_delimiter = '{{'
parameter_end_delimiter = '}}'
tag_file_name = '$filename'
tag_schema_id = '$schema'
tag_array_parent = '$array_parent'
tag_object_parent = '$object_parent'
tag_kind_parent = '$kind_parent'
tag_required_template = '$required_template'
tag_optional_field = '$'

pattern_one_of = r'\$\$_oneOf_[1-9][0-9]*\$\$'
pattern_any_of = r'\$\$_anyOf_[1-9][0-9]*\$\$'
pattern_remove = re.compile('('
                            + pattern_one_of
                            + '|'
                            + pattern_any_of
                            + ')')


def load_schemas(schema_path, schema_ns_name=None, schema_ns_value=None):
    def list_schema_files(path, file_list):
        files = os.listdir(path)
        for file in files:
            full_path = os.path.join(path, file)
            if os.path.isfile(full_path):
                if file.endswith('.json'):
                    file_list.append(full_path)
            elif os.path.isdir(full_path):
                list_schema_files(full_path, file_list)

    dict_schemas = dict()

    # load all json files
    file_list = []
    list_schema_files(schema_path, file_list)
    for schema_file in file_list:
        with open(schema_file, 'r', encoding='utf-8') as fp:
            a_schema = json.load(fp)
            if schema_ns_name is not None and len(schema_ns_name) > 0:
                a_schema = replace_json_namespace(a_schema, schema_ns_name+":", schema_ns_value+":")
            id = a_schema.get('$id')
            if id is None:
                id = a_schema.get('$ID')
            if id is not None:
                dict_schemas[id] = a_schema

            # for top level resource,
            if a_schema.get('properties', {}).get('ResourceHomeRegionID') is not None:
                # remove 'required"
                if a_schema.get('required') is not None:
                    del a_schema['required']
                # remove 'additionalProperties"
                if a_schema.get('additionalProperties') is not None:
                    del a_schema['additionalProperties']
                # remove resource/type id version
                if a_schema.get('properties', {}).get('ResourceTypeID', {}).get('pattern') is not None:
                    a_schema['properties']['ResourceTypeID']['pattern'] = \
                        a_schema['properties']['ResourceTypeID']['pattern'].replace(':[0-9]+', ':[0-9]*')
                if a_schema.get('properties', {}).get('ResourceID', {}).get('pattern') is not None:
                    a_schema['properties']['ResourceID']['pattern'] = \
                        a_schema['properties']['ResourceID']['pattern'].replace(':[0-9]+', ':[0-9]*')

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


def replace_json_namespace(json_obj, ns_name, ns_value):
    if ns_name is not None and len(ns_name) > 0:
        json_str = json.dumps(json_obj)
        return json.loads(json_str.replace(ns_name, ns_value))

    return json_obj


def parse_template_parameters(root_object, parameters_object):
    def parse_str(root_object, keys, val, root_list, parameters_object):
        val = val.strip()
        parameter_start_index = val.find(parameter_start_delimiter)
        parameter_end_index = val.find(parameter_end_delimiter)
        if parameter_start_index >= 0 and parameter_end_index >= 0:
            parameter = val[parameter_start_index:parameter_end_index + len(parameter_end_delimiter)]
            if len(parameter) > 0:
                parameter_recs = parameters_object.get(parameter)
                if parameter_recs is None:
                    parameter_recs = []
                    parameters_object[parameter] = parameter_recs
                parameter_recs.append((root_object, keys, root_list))
            val_left = val[parameter_end_index + len(parameter_end_delimiter):]
            if len(val_left) > 0:
                parse_str(root_object, keys, val_left, root_list, parameters_object)

    def parse_dict(parent_object, root_object, keys, root_list, parameters_object):
        for key, val in parent_object.items():
            new_keys = list(keys)
            new_keys.append(key)
            if type(val) is dict:
                parse_dict(val, root_object, new_keys, root_list, parameters_object)

            if type(val) is list:
                if len(val) == 1:
                    new_root_list = list(root_list)
                    new_root_list.append((root_object, new_keys))
                    new_root_object = {0: val[0]}
                    parse_dict(new_root_object, new_root_object, [], new_root_list, parameters_object)

            if type(val) is str:
                f_root_list = list(root_list)
                parse_str(root_object, new_keys, val, f_root_list, parameters_object)

    parse_dict(root_object, root_object, [], [], parameters_object)


def map_csv_column_names_to_parameters(csv_file, parameters_object):
    def get_column_index(col_names, col_name):
        idx = -1
        try:
            idx = col_names.index(col_name)
            # check duplicate
            col_names.index(col_name, idx + 1)
            raise Exception('Duplicate parameter found in csv file:', col_name)
        except ValueError:
            pass

        return idx

    column_names = list()
    with open(csv_file, mode='r') as infile:
        reader = csv.reader(infile)
        for rows in reader:
            for colname in rows:
                column_names.append(colname.strip().lower())
            break

    map_parameter_column = dict()
    for parameter, parameter_recs in parameters_object.items():
        parameter_key = parameter[len(parameter_start_delimiter):-len(parameter_end_delimiter)].strip().lower()
        for parameter_rec in parameter_recs:
            _, _, root_list = parameter_rec
            d_array = len(root_list)
            if d_array == 0:
                # parameters not inside array type
                parameter_column = map_parameter_column.get(parameter)
                if parameter_column is None:
                    csv_index = get_column_index(column_names, parameter_key)
                    if csv_index >= 0:
                        map_parameter_column[parameter] = csv_index
            else:
                # parameters inside array type
                parameter_column = map_parameter_column.get(parameter)
                if parameter_column is not None:
                    raise Exception('Duplicate array parameter not allowed:', parameter)
                parameter_column = []
                map_parameter_column[parameter] = parameter_column

                # find max indexes
                indexes_count = [0 for i in range(d_array)]
                pattern = re.escape(parameter_key)
                for i in range(d_array):
                    pattern = pattern + '_[1-9][0-9]*'

                for column_name in column_names:
                    if re.fullmatch(pattern, column_name):
                        temp_name = column_name[(len(parameter_key)+1):]
                        col_nums = [int(n) for n in temp_name.split('_')]
                        for i in range(d_array):
                            indexes_count[i] = max(indexes_count[i], col_nums[i])

                # find csv column index for each parameter array indexes
                indexes_column = [0 for i in range(d_array)]
                indexes_column.append(-1)
                done = False
                count = 0
                while not done:
                    count = count + 1
                    parameter_with_indexes = parameter_key
                    for i in range(d_array):
                        parameter_with_indexes = parameter_with_indexes + '_' + str(indexes_column[i] + 1)
                    csv_index = get_column_index(column_names, parameter_with_indexes)
                    if csv_index >= 0:
                        indexes_column[d_array] = csv_index
                        parameter_column.append(list(indexes_column))
                    for i in range(d_array-1, -1, -1):
                        temp_val = indexes_column[i] + 1
                        if temp_val < indexes_count[i]:
                            for j in range(d_array-1, i-1, -1):
                                indexes_column[j] = 0
                            indexes_column[i] = temp_val
                            break
                        done = (i == 0)

    return map_parameter_column


def get_deepest_key_object(root_object, keys):
    if keys is None or len(keys) == 0:
        return {0: root_object}, 0

    parent_object = root_object
    key = keys[0]
    for k in keys[1:]:
        parent_object = parent_object[key]
        key = k

    return parent_object, key


def replace_parameter_with_data(root_object, keys, parameter, data_row, col_index):
    data = ''
    if col_index is not None and col_index > -1:
        data = data_row[col_index].strip()
    if len(data) == 0:
        # ignore it, will clean up later
        return

    parent_object, key = get_deepest_key_object(root_object, keys)

    result = parent_object[key].replace(parameter, data)

    # check if this needs be evaluated
    pattern = r'(int|float|bool|datetime_YYYY-MM-DD|datetime_MM/DD/YYYY)\(' + re.escape(parameter) + r'\)'
    if re.fullmatch(pattern, parent_object[key].strip()):
        if result.startswith('bool'):
            parent_object[key] = (data.lower() in ('true','yes','y','t','1'))
        elif result.startswith('datetime_YYYY-MM-DD'):
            input_date_time = datetime.datetime.strptime(data, '%Y-%m-%d')
            parent_object[key] = input_date_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif result.startswith('datetime_MM/DD/YYYY'):
            parent_object[key] = datetime.datetime.strptime(data, '%m/%d/%Y').isoformat()
        elif result.startswith('int'):
            parent_object[key] = int(data)
        elif result.startswith('float'):
            parent_object[key] = float(data)
    else:
        parent_object[key] = result

def get_root_object(root_object_key_list, indexes):
    root_object, root_keys = root_object_key_list[0]
    parent_object, key = get_deepest_key_object(root_object, root_keys)
    array_object = parent_object[key]
    return_root = None
    return_keys = None

    for i in range(len(indexes)):
        index = indexes[i]
        child_object_template, child_keys = root_object_key_list[i+1]
        if child_keys[0] == 0:
            child_object_template = child_object_template[child_keys[0]]
            child_keys = child_keys[1:]
        while len(array_object) <= index:
            array_object.append(copy.deepcopy(child_object_template))
        return_root = array_object[index]
        return_keys = child_keys
        if type(return_root) is str:
            return_root = array_object
            return_keys = [index]

        parent_object, key = get_deepest_key_object(return_root, return_keys)
        array_object = parent_object[key]

    return return_root, return_keys


def clear_non_filled_parameters(ldm):
    def clear_list(lst):
        for i in range(len(lst) - 1, -1, -1):
            list_val = lst[i]
            if type(list_val) is dict:
                clear_dict(list_val)
            elif type(list_val) is list:
                clear_list(list_val)
            elif type(list_val) is str:
                parameter_start_index = list_val.find(parameter_start_delimiter)
                parameter_end_index = list_val.find(parameter_end_delimiter)
                if parameter_start_index >= 0 and parameter_end_index >= 0:
                    # delete it
                    del lst[i]

        # remove extra empty item in list
        def is_item_empty(item):
            item_is_empty = True
            if type(item) is dict:
                for key, val in item.items():
                    if not is_item_empty(val):
                        item_is_empty = False
                        break
            elif type(item) is list:
                for val in item:
                    if not is_item_empty(val):
                        item_is_empty = False
                        break
            else:
                item_is_empty = False
            return item_is_empty

        # remove extra empty item in list
        for i in range(len(lst) - 1, -1, -1):
            if is_item_empty(lst[i]):
                    del lst[i]

    def clear_dict(d):
        to_be_deleted = []
        for key, val in d.items():
            if type(val) is dict:
                clear_dict(val)
                if len(val) == 0:
                    # delete it
                    to_be_deleted.append(key)
            elif type(val) is list:
                clear_list(val)
                if len(val) == 0:
                    # delete it
                    to_be_deleted.append(key)
            elif type(val) is str:
                parameter_start_index = val.find(parameter_start_delimiter)
                parameter_end_index = val.find(parameter_end_delimiter)
                if parameter_start_index >= 0 and parameter_end_index >= 0:
                    # delete it
                    to_be_deleted.append(key)
        for del_key in to_be_deleted:
            del d[del_key]

    clear_dict(ldm)


def remove_special_tags(ldm):
    def remove_list_tags(lst):
        for item in lst:
            if type(item) is dict:
                remove_dict_tags(item)
            elif type(item) is list:
                remove_list_tags(item)

    def remove_dict_tags(d):
        to_be_replaced = dict()
        for key, val in d.items():
            new_key = pattern_remove.sub('', key)
            if new_key != key:
                to_be_replaced[key] = new_key
            if type(val) is dict:
                remove_dict_tags(val)
            elif type(val) is list:
                remove_list_tags(val)
        for old_key, new_key in to_be_replaced.items():
            if d.get(new_key) is not None:
                raise Exception('Detected duplicate attributes:', old_key)
            d[new_key] = d.pop(old_key)

    remove_dict_tags(ldm)


def add_required_fields(ldm, required_template):
    if required_template is None or len(required_template) == 0:
        return

    def add_fields_list(des_list, src_list):
        if len(src_list) == 0 or len(des_list) == 0:
            return

        for item_src in src_list:
            for item_des in des_list:
                if type(item_src) == type(item_des):
                    if type(item_src) is dict:
                        add_fields_dict(item_des, item_src)
                    elif type(item_src) is list:
                        add_fields_list(item_des, item_src)

    def add_fields_dict(des_dict, src_dict):
        for k, v_src in src_dict.items():
            optional_field = False
            if k.startswith(tag_optional_field):
                optional_field = True
                k = k[len(tag_optional_field):]
            if k not in des_dict and not optional_field:
                if type(v_src) is dict:
                    des_dict[k] = dict()
                elif type(v_src) is list:
                    des_dict[k] = list()
                else:
                    des_dict[k] = v_src

            v_des = des_dict.get(k, None)
            if v_des is not None and type(v_src) == type(v_des):
                if type(v_src) is dict:
                    add_fields_dict(v_des, v_src)
                elif type(v_src) is list:
                    add_fields_list(v_des, v_src)

    add_fields_dict(ldm, required_template)


def set_acl_values(manifest, acl_viewer=None, acl_owner=None):
    """Set ACL values in the manifest using lowercase 'acl' key only"""
    if not isinstance(manifest, dict):
        return

    # Remove any existing ACL section to avoid duplicates
    manifest.pop('acl', None)

    acl = {}
    manifest['acl'] = acl

    if acl_viewer:
        acl['viewers'] = [acl_viewer]
    if acl_owner:
        acl['owners'] = [acl_owner]

def set_legal_tag(manifest, legal_tag):
    """Set legal tag in the manifest with correct casing"""
    if not isinstance(manifest, dict):
        return

    legal = manifest.get('legal')
    legal['legaltags'] = [legal_tag]
    legal['otherRelevantDataCountries'] = ["US"]


def create_manifest_from_row(root_template, required_template,
                             parameters_object, map_parameter_column, data_row,
                             acl_viewer=None, acl_owner=None, legal_tag=None):
    # create new instance from template
    ldm = copy.deepcopy(root_template)

    new_parameters_object = dict()
    parse_template_parameters(ldm, new_parameters_object)

    assert new_parameters_object == parameters_object

    # empty top-level array object
    for parameter, parameter_recs in new_parameters_object.items():
        for parameter_rec in parameter_recs:
            _, _, root_list = parameter_rec
            if len(root_list) > 0:
                top_object, top_keys = root_list[0]
                parent_object, key = get_deepest_key_object(top_object, top_keys)
                parent_object[key] = []


    # replace parameters with data
    for parameter, parameter_recs in new_parameters_object.items():
        for parameter_rec in parameter_recs:
            root_object, keys, root_list = parameter_rec
            d_array = len(root_list)
            if d_array == 0:
                # parameters not inside array type
                parameter_column = map_parameter_column.get(parameter)
                replace_parameter_with_data(root_object, keys, parameter, data_row, parameter_column)
            else:
                # parameters inside array type
                parameter_column = map_parameter_column.get(parameter)

                # create array objects
                new_root_list = list(root_list)
                new_root_list.append((root_object, keys))
                for indexes_column in parameter_column:
                    new_root_object, new_keys = get_root_object(new_root_list, indexes_column[:-1])
                    replace_parameter_with_data(new_root_object, new_keys, parameter, data_row, indexes_column[-1])

    # clean up non-filled parameters
    clear_non_filled_parameters(ldm)

    # clear special tags (oneOf, anyOf etc)
    remove_special_tags(ldm)

    # add required fields if needed
    add_required_fields(ldm, required_template)

    # set ACL values if provided
    if acl_viewer or acl_owner:
        set_acl_values(ldm, acl_viewer, acl_owner)
    
    # set legal tag if provided
    if legal_tag:
        set_legal_tag(ldm, legal_tag)

    return ldm


def create_manifest_from_csv(input_csv, template_json, output_path,
                             schema_path=None, schema_ns_name=None, schema_ns_value=None,
                             required_template=None, array_parent=None, object_parent=None, group_filename=None,
                             acl_viewer=None, acl_owner=None, legal_tag=None):
    with open(template_json, 'r') as fp:
        root_template = json.load(fp)

    schema_id = root_template.get(tag_schema_id)
    if schema_id is not None:
        del root_template[tag_schema_id]

    required_template_from_template = root_template.get(tag_required_template)
    if required_template_from_template is not None:
        del root_template[required_template_from_template]
    if required_template is not None and len(required_template) > 0:
        required_template = json.loads(required_template)
    else:
        required_template = required_template_from_template

    output_array_parent = root_template.get(tag_array_parent)
    if output_array_parent is not None:
        del root_template[tag_array_parent]
    if array_parent is not None and len(array_parent) > 0:
        output_array_parent = array_parent

    output_object_parent = root_template.get(tag_object_parent)
    if output_object_parent is not None:
        del root_template[tag_object_parent]
    if object_parent is not None and len(object_parent) > 0:
        output_object_parent = object_parent

    output_kind_parent = root_template.get(tag_kind_parent)
    if output_kind_parent is not None:
        del root_template[tag_kind_parent]
        if schema_ns_name is not None and len(schema_ns_name) > 0:
            output_kind_parent = output_kind_parent.replace(schema_ns_name + ":", schema_ns_value + ":")

    group_lm = None
    group_lm_parent = None
    if group_filename is not None and len(group_filename) > 0:
        if output_array_parent is None or len(output_array_parent) == 0:
            logging.warning("Array parent is needed to group generated load manifests")

        if not group_filename.endswith('.json'):
            group_filename = group_filename + '.json'
        output_group_filename = os.path.join(output_path, group_filename)
        group_lm = dict()
        if output_kind_parent is not None and len(output_kind_parent) > 0:
            group_lm['kind'] = output_kind_parent
        parent_items = output_array_parent.split(".")
        group_lm_parent = group_lm
        for parent_item in parent_items[:-1]:
            parent_item = parent_item.strip()
            group_lm[parent_item] = dict()
            group_lm_parent = group_lm_parent[parent_item]
        group_lm_parent[parent_items[-1]] = []
        group_lm_parent = group_lm_parent[parent_items[-1]]

    # load schemas if available
    do_validate = (schema_path is not None and len(schema_path) > 0 and schema_id is not None)
    dict_schemas = dict()
    schema = None
    wp_wpc_schema = None
    if do_validate:
        dict_schemas = load_schemas(schema_path, schema_ns_name, schema_ns_value)
        schema = dict_schemas.get(schema_id)
        if schema is None:
            logging.warning("No schema found for: %s", schema_id)
        else:
            wp_wpc_schema = schema.get('properties', {}).get('Data', None)
            if wp_wpc_schema is not None and wp_wpc_schema.get('properties', {}).get('WorkProduct') is not None:
                schema = wp_wpc_schema
                schema['$id'] = schema_id

    parameters_object = dict()
    parse_template_parameters(root_template, parameters_object)

    # read csv column names
    map_parameter_column = map_csv_column_names_to_parameters(input_csv, parameters_object)

    # output each csv row as one json file
    with open(input_csv, mode='r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        skip_first_row = True
        row_count = 0
        processed = set()
        processed_lower = set()
        empty_row_count = 0
        for rows in reader:
            if skip_first_row:
                skip_first_row = False
                continue

            row_count = row_count + 1
            # skip empty row
            skip_empty_row = True

            for row_column in rows:
                if row_column is not None and len(row_column.strip()) > 0:
                    skip_empty_row = False
                    break
            if skip_empty_row:
                empty_row_count = empty_row_count + 1
                continue

            output_file = os.path.join(output_path, os.path.basename(input_csv)[:-4]+'_'+str(row_count)+'.json')
            try:
                lm = create_manifest_from_row(
                            root_template = root_template,
                            required_template=required_template,
                            parameters_object=parameters_object,
                            map_parameter_column=map_parameter_column,
                            data_row=rows,
                            acl_viewer=acl_viewer,
                            acl_owner=acl_owner,
                            legal_tag=legal_tag
                        )
                if schema_ns_name is not None and len(schema_ns_name) > 0:
                    lm = replace_json_namespace(lm, schema_ns_name + ":", schema_ns_value + ":")
                output_file_name = lm.get(tag_file_name)
                output_file_name = output_file_name.replace('/', '-')
                output_file_name = output_file_name.replace('\\', '-')

                if output_file_name is not None:
                    output_file = os.path.join(output_path, output_file_name)
                    del lm[tag_file_name]
                if output_file in processed:
                    logging.warning("Duplicate rows found. Row: %s %s %s", row_count, ", File name:", output_file_name)
                else:
                    # Windows file name is not case-sensitive
                    duplicate_name_count = 1
                    while output_file.lower() in processed_lower:
                        name_parts = output_file.split('.')
                        if len(name_parts) > 1:
                            name_parts[-2] = name_parts[-2] + '_' + str(duplicate_name_count)
                            output_file = '.'.join(name_parts)
                        else:
                            output_file = output_file + '_' + str(duplicate_name_count)
                        duplicate_name_count = duplicate_name_count + 1

                if output_array_parent is not None:
                    if group_lm_parent is not None:
                        group_lm_parent.append(lm)
                    else:
                        parent_items = output_array_parent.split(".")
                        new_lm = dict()
                        if output_kind_parent is not None and len(output_kind_parent) > 0:
                            new_lm['kind'] = output_kind_parent
                        lm_parent = new_lm
                        for parent_item in parent_items[:-1]:
                            parent_item = parent_item.strip()
                            lm_parent[parent_item] = dict()
                            lm_parent = lm_parent[parent_item]
                        lm_parent[parent_items[-1]] = [lm]
                        lm = new_lm
                elif output_object_parent is not None:
                    parent_items = output_object_parent.split(".")
                    new_lm = dict()
                    if output_kind_parent is not None and len(output_kind_parent) > 0:
                        new_lm['kind'] = output_kind_parent
                    lm_parent = new_lm
                    for parent_item in parent_items[:-1]:
                        parent_item = parent_item.strip()
                        lm_parent[parent_item] = dict()
                        lm_parent = lm_parent[parent_item]
                    lm_parent[parent_items[-1]] = lm
                    lm = new_lm

                if group_lm is None:
                    with open(output_file, "w") as f:
                        json.dump(
                            obj=lm,
                            fp=f,
                            indent=4
                        )

                processed.add(output_file)
                processed_lower.add(output_file.lower())
            except Exception:
                logging.exception("Unable to process data row: {}".format(row_count))
                try:
                    os.remove(output_file)
                except Exception:
                    pass

        if group_lm is not None:
            with open(output_group_filename, "w") as f:
                json.dump(
                    obj=group_lm,
                    fp=f,
                    indent=4
                )

        logging.info("Generated {} load manifests.".format(len(processed)))
        if empty_row_count > 0:
            logging.info("Skipped {} empty rows.".format(empty_row_count))