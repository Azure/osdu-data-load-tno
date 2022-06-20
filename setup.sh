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

#!/usr/bin/env bash

mkdir -p $HOME/.osducli
CONFIG_FILE=$HOME/.osducli/config

cat > $CONFIG_FILE << EOF
[core]
server = ${OSDU_ENDPOINT}
crs_catalog_url = /api/crs/catalog/v2/
crs_converter_url = /api/crs/converter/v2/
entitlements_url = /api/entitlements/v2/
file_url = /api/file/v2/
legal_url = /api/legal/v1/
schema_url = /api/schema-service/v1/
search_url = /api/search/v2/
storage_url = /api/storage/v2/
unit_url = /api/unit/v3/
workflow_url = /api/workflow/v1/
data_partition_id = ${DATA_PARTITION}
legal_tag = ${LEGAL_TAG}
acl_viewer = data.default.viewers@p${DATA_PARTITION}.contoso.com
acl_owner = data.default.owners@${DATA_PARTITION}.contoso.com
authentication_mode = refresh_token
token_endpoint = https://login.microsoftonline.com/${AZURE_TENANT}/oauth2/v2.0/token
refresh_token = ${REFRESH_TOKEN}
client_id = ${CLIENT_ID}
client_secret = ${CLIENT_SECRET}
EOF

chmod 600 $CONFIG_FILE
