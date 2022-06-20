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

import configparser
import requests

from utils import get_headers

# Read config file dataload.ini
config = configparser.RawConfigParser()
config.read("output/dataload.ini")

# Some constants, used by script
LEGAL_URL = config.get("CONNECTION", "legal_url")
LEGAL_TAG = config.get("REQUEST", "legal_tag")
DATA_PARTITION_ID = config.get("CONNECTION", "data-partition-id")


def verify_legal_tag_name():
    """
    Validate Legal Tag exists.
    """
    headers = get_headers(config)
    try:
        endpoint = f"{LEGAL_URL}/legaltags/{LEGAL_TAG}"
        print(endpoint)
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)


def main():
    """
    Main Entry Function.
    """
    verify_legal_tag_name()


if __name__ == "__main__":
    main()
