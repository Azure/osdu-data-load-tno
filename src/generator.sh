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

#!/bin/bash
GROUP_FILE=false
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PARENT_DIR=`dirname $SCRIPT_DIR`
home_directory=$PARENT_DIR

while getopts m:d:t:o:g:h:p: flag
do
    case "${flag}" in
        m) mapping_file=${OPTARG};;
        d) data_file_directory=${OPTARG};;
        t) template_file_directory=${OPTARG};;
        o) output_directory=${OPTARG};;
        g) GROUP_FILE=true;;
        h) home_directory=${OPTARG};;
        p) data_partition=${OPTARG};;
    esac
done

schema_namespace="<namespace>"
schema_ns_value="${data_partition}"

required_template="$(jq '.required_template' ${mapping_file})"

# if the while loop does not work for you, use corresponding readarray command
# readarray -t my_array < <(jq -c '.mapping[]' ${mapping_file})
while IFS=\= read val; do my_array+=($val); done < <(jq -c '.mapping[]' ${mapping_file})

# iterate through the Bash array
for item in "${my_array[@]}"; do
    # TODO: Parameterize the input file location..
    data_file="${home_directory}/open-test-data/TNO/contrib/${data_file_directory}/$(jq -r '.data_file' <<< $item)"
    template_file="${home_directory}/open-test-data/templates/${template_file_directory}/$(jq -r '.template_file' <<< $item)"

    echo "Creating manifest using $data_file with template $template_file"

    if [ "$GROUP_FILE" = false ] ; then
        group_name_param=""
    else
        output_file="$(jq -r '.output_file_name' <<< $item)"
        group_name_param="--group_filename ${output_file}"
    fi

    python3 -m loading_manifest.main_smds --input_csv ${data_file} --template_json ${template_file} \
            --output_path "${home_directory}/${output_directory}" --schema_path "${home_directory}/open-test-data/schema" --schema_ns_name ${schema_namespace} \
            --schema_ns_value ${schema_ns_value} --required_template "${required_template}" \
            ${group_name_param}

done
