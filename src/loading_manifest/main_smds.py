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
import json
import logging
import loading_manifest.csv_to_json as m_csv_to_json_converter


def main(argv):
    input_csv = ""
    template_json = ""
    output_path = ""
    schema_path = ""
    schema_ns_name = ""
    schema_ns_value = ""
    required_template = ""
    array_parent = ""
    object_parent = ""
    group_filename = ""

    usage = "usage: python -m loading_manifest.main_smds " \
            "--input_csv <inputcsv> " \
            "--template_json <template> " \
            "--output_path <outputpath> " \
            "--schema_path <schemapath> " \
            "--schema_ns_name <schema_ns_name> " \
            "--schema_ns_value <schema_ns_value " \
            "--required_template <required_template> " \
            "--array_parent <array_parent> " \
            "--object_parent <object_parent>" \
            "--group_filename <group_filename> "

    try:
        opts, args = getopt.getopt(argv, "h",
                                   ["input_csv=", "template_json=", "output_path=",
                                    "schema_path=", "schema_ns_name=", "schema_ns_value=",
                                    "required_template=", "array_parent=", "object_parent=", "group_filename="])
    except getopt.GetoptError as e:
        logging.info(usage)
        logging.exception(str(e))
        return

    for opt, arg in opts:
        if opt == '-h':
            print(usage)
            return
        elif opt == "--input_csv":
            input_csv = arg.strip()
            if len(input_csv) > 0:
                input_csv = os.path.abspath(input_csv)
        elif opt == "--template_json":
            template_json = arg.strip()
            if len(template_json) > 0:
                template_json = os.path.abspath(template_json)
        elif opt == "--output_path":
            output_path = arg.strip()
            if len(output_path) > 0:
                output_path = os.path.abspath(output_path)
        elif opt == "--schema_path":
            schema_path = arg.strip()
            if len(schema_path) > 0:
                schema_path = os.path.abspath(schema_path)
        elif opt == "--schema_ns_name":
            schema_ns_name = arg.strip()
        elif opt == "--schema_ns_value":
            schema_ns_value = arg.strip()
        elif opt == "--required_template":
            required_template = arg.strip()
        elif opt == "--array_parent":
            array_parent = arg.strip()
        elif opt == "--object_parent":
            object_parent = arg.strip()
        elif opt == "--group_filename":
            group_filename = arg.strip()

    if input_csv is not None and len(input_csv) > 0:
        logging.debug("input_csv: %s", input_csv)
    if template_json is not None and len(template_json) > 0:
        logging.debug("template_json: %s", template_json)
    if output_path is not None and len(output_path) > 0:
        logging.debug("output_path: %s", output_path)
    if schema_path is not None and len(schema_path) > 0:
        logging.debug("schema_path: %s", schema_path)
    if schema_ns_name is not None and len(schema_ns_name) > 0:
        logging.debug("schema_ns_name: %s", schema_ns_name)
    if schema_ns_value is not None and len(schema_ns_value) > 0:
        logging.debug("schema_ns_value: %s", schema_ns_value)
    if required_template is not None and len(required_template) > 0:
        required_template = json.dumps(json.loads(required_template), indent=2)
        logging.debug("required_template: %s", required_template)
    if array_parent is not None and len(array_parent) > 0:
        logging.debug("array_parent: %s", array_parent)
    if object_parent is not None and len(object_parent) > 0:
        logging.debug("object_parent: %s", object_parent)
    if group_filename is not None and len(group_filename) > 0:
        logging.debug("group_filename: %s", group_filename)

    logging.debug("\n")

    if len(input_csv) == 0 \
        or len(template_json) == 0 \
            or len(output_path) == 0:
        logging.error(usage)
        return

    # create output path if it does not exist
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    m_csv_to_json_converter.create_manifest_from_csv(
        input_csv=input_csv,
        template_json=template_json,
        output_path=output_path,
        schema_path=schema_path,
        schema_ns_name=schema_ns_name,
        schema_ns_value=schema_ns_value,
        required_template=required_template,
        array_parent=array_parent,
        object_parent=object_parent,
        group_filename=group_filename
    )


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
