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
from datetime import datetime
import json
import logging
import os
import sys
import time
from urllib.error import HTTPError
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import concurrent.futures
import argparse
from pprint import pformat
from utils import get_headers
import urllib
from azure.storage.blob import BlobClient, __version__
from joblib import Parallel, delayed
import multiprocessing
import fnmatch
import re


MAX_CHUNK_SIZE = 32  # MegaBytes
DEFAULT_TIMEOUT = 5  # seconds


# Read config file dataload.ini
config = configparser.RawConfigParser()
config.read("output/dataload.ini")

# Setup File Ingestion Includes
includes = ['*.pdf', '*.csv', '*.las', '*.txt']  # for files only
includes = r'|'.join([fnmatch.translate(x) for x in includes])

# Set up base logger
timestamp = datetime.now().strftime('%m%d%y')
LOG_FILE_NAME = "dataloader-{}.log".format(timestamp)
LOG_FILE_PATH = os.path.join(os.getcwd(), "output", LOG_FILE_NAME)
os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
print("Saving operation logs to", LOG_FILE_PATH)

handler = logging.FileHandler(LOG_FILE_PATH)
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(name)-14.14s] [%(levelname)-7.7s]  %(message)s"))
logger = logging.getLogger("Dataload")

try:
    level = logging.getLevelName(os.environ.get('LOG_LEVEL', 'info').upper())
    logger.setLevel(level)
except ValueError:
    print('Valid Log Levels are DEBUG, INFO, WARN and ERROR')
    exit(1)

logger.addHandler(handler)

# Set up file logger
LOG_FILENAME = 'output/execution.log'
RESULTS_FILENAME = "output/results.json"

START_TIME = "startTimeStamp"
END_TIME = "endTimeStamp"
STATUS = "status"
RUN_ID = "runId"
TIME_TAKEN = "timeTaken"
FINISHED = "finished"
FAILED = "failed"

FILE_UPLOAD_SUCCESS = 0
FILE_UPLOAD_FAILED = 1

handler = logging.FileHandler(LOG_FILENAME)
handler.setFormatter(logging.Formatter("%(message)s"))
file_logger = logging.getLogger("Execution")
file_logger.setLevel(logging.INFO)
file_logger.addHandler(handler)

# Some constants, used by script
SCHEMAS_URL = config.get("CONNECTION", "schemas_url")
STORAGE_URL = config.get("CONNECTION", "storage_url")
WORKFLOW_URL = config.get("CONNECTION", "workflow_url")
SEARCH_URL = config.get("CONNECTION", "search_url")
FILE_URL = config.get("CONNECTION", "file_url")

LEGAL_TAG = config.get("REQUEST", "legal_tag")
DATA_PARTITION_ID = config.get("CONNECTION", "data-partition-id")

SEARCH_OK_RESPONSE_CODES = [200]
DATA_LOAD_OK_RESPONSE_CODES = [200]
NOT_FOUND_RESPONSE_CODES = [404]
BAD_TOKEN_RESPONSE_CODES = [400, 401, 403, 500]


def requests_retry_session(
    retries=5,
    backoff_factor=1.5,
    status_forcelist=(404, 429, 500, 502, 503, 504),
    allowed_methods=["GET", "PUT", "POST", "DELETE"],
    timeout=10,  # Timeout in seconds
    session=None,
):
    session = session or requests.Session()

    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(allowed_methods),  # Convert to frozenset for consistency with Retry class
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # Wrap the request method with a default timeout
    original_request = session.request

    def request_with_timeout(method, url, **kwargs):
        kwargs.setdefault('timeout', timeout)  # Set default timeout if not provided
        return original_request(method, url, **kwargs)

    session.request = request_with_timeout

    return session

def get_directory_size(directory):
    """Returns the `directory` size in bytes."""
    total = 0
    try:
        # print("[+] Getting the size of", directory)
        for entry in os.scandir(directory):
            if entry.is_file():
                # if it's a file, use stat() function
                total += entry.stat().st_size
            elif entry.is_dir():
                # if it's a directory, recursively call this function
                try:
                    total += get_directory_size(entry.path)
                except FileNotFoundError:
                    pass
    except NotADirectoryError:
        # if `directory` isn't a directory, get the file size then
        return os.path.getsize(directory)
    except PermissionError:
        # if for whatever reason we can't open the folder, return 0
        return 0
    return total


def get_size_format(b, factor=1024, suffix="B"):
    """
    Scale bytes to its proper byte format
    e.g:
        1253656 => '1.20MB'
        1253656678 => '1.17GB'
    """
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if b < factor:
            return f"{b:.2f}{unit}{suffix}"
        b /= factor
    return f"{b:.2f}Y{suffix}"


def delete_ingested_records(dir_name):
    # Recursive traversal of files and subdirectories of the root directory and files processing
    success = []
    failed = []
    for root, _, files in os.walk(dir_name):
        for file in files:
            filepath = os.path.join(root, file)
            if filepath.endswith(".json"):
                with open(filepath) as file:
                    data_object = json.load(file)

            if not data_object:
                logger.error(f"Error with file {filepath}. File is empty.")
            elif "ReferenceData" in data_object:
                ingested_data = data_object["ReferenceData"]
            elif "MasterData" in data_object:
                ingested_data = data_object["MasterData"]

            ids = []
            for ingested_datum in ingested_data:
                if "id" in ingested_datum:
                    ids.append(ingested_datum.get("id"))

            s, f = delete_ids(ids)
            success += s
            failed += f
            logger.info(f"deleted records of - {filepath}")

    return success, failed


def verify_references(dir_name, batch_size=20, ingestion_sequence=""):
    success = []
    failed = []
    with open(ingestion_sequence) as file:
        sequence = json.load(file)

    for entry in sequence:
        record_ids = []
        fileName = entry.get("FileName")
        filepath = os.path.join(dir_name, fileName)
        filepath_normalized = os.path.normpath(filepath)
        logger.debug(f"Verifying file: {filepath_normalized}")
        if filepath_normalized.endswith(".json"):
            with open(filepath_normalized) as file:
                data_object = json.load(file)
                ingested_data = data_object["ReferenceData"]
        else:
            return

        cur_batch = 0

        for ingested_datum in ingested_data:
            if "id" in ingested_datum:
                record_ids.append(reference_data_id(ingested_datum))
                cur_batch += 1

            if cur_batch >= batch_size:
                logger.debug(f"Searching records with batch size {cur_batch}")
                s, f = verify_ids(record_ids)
                success += s
                failed += f
                cur_batch = 0
                record_ids = []

        if cur_batch > 0:
            logger.debug(
                f"Searching remaining records with batch size {cur_batch}")
            s, f = verify_ids(record_ids)
            success += s
            failed += f

    return success, failed


def verify_ingestion(dir_name, batch_size=1):
    # Recursive traversal of files and subdirectories of the root directory and files processing

    success = []
    failed = []

    reference_pattern = "{}:reference-data".format(config.get("CONNECTION", "data-partition-id"))
    master_pattern = "{}:master-data".format(config.get("CONNECTION", "data-partition-id"))
    sleep_after_count = 1000
    queries_made = 0

    for root, _, files in os.walk(dir_name):
        logger.debug(f"Files list: {files}")
        cur_batch = 0
        record_ids = []
        for file in files:
            filepath = os.path.join(root, file)
            if filepath.endswith(".json"):
                with open(filepath) as file:
                    data_object = json.load(file)

            else:
                continue
            if not data_object:
                logger.error(f"Error with file {filepath}. File is empty.")

            elif "ReferenceData" in data_object and len(data_object["ReferenceData"]) > 0:
                ingested_data = data_object["ReferenceData"]

            elif "MasterData" in data_object and len(data_object["MasterData"]) > 0:
                ingested_data = data_object["MasterData"]

            elif "Data" in data_object:
                ingested_data = data_object["Data"]

            if isinstance(ingested_data, dict):
                if id not in ingested_data["WorkProduct"]:
                    # Add the Work-Product Id -> opendes:work-product--WorkProduct:load_document_69_D_CH_11_pdf.json
                    work_product_id = generate_workproduct_id(ingested_data["WorkProduct"]["data"]["Name"],
                                                              get_directory_name(filepath))
                    record_ids.append(work_product_id)
                else:
                    record_ids.append(ingested_datum.get("id").replace('osdu:reference-data', reference_pattern).replace('osdu:master-data', master_pattern))
                cur_batch += 1

            elif isinstance(ingested_data, list):
                for ingested_datum in ingested_data:
                    if "id" in ingested_datum:
                        record_ids.append(reference_data_id(ingested_datum, reference_pattern, master_pattern))
                        cur_batch += 1

            if cur_batch >= batch_size:
                logger.debug(f"Searching records with batch size {cur_batch}")
                s, f = verify_ids(record_ids)
                success += s
                failed += f
                queries_made = queries_made + cur_batch
                if queries_made >= sleep_after_count:
                    time.sleep(60)
                    queries_made = 0

                cur_batch = 0
                record_ids = []
            else:
                logger.debug(
                    f"Current batch size after process {filepath} is {cur_batch}. Reading more files..")

        if cur_batch > 0:
            logger.debug(
                f"Searching remaining records with batch size {cur_batch}")
            s, f = verify_ids(record_ids)
            success += s
            failed += f
            queries_made = queries_made + cur_batch
            if queries_made >= sleep_after_count:
                time.sleep(60)
                queries_made = 0

    return success, failed


def delete_ids(ids):
    success = []
    failed = []
    for id in ids:
        headers = get_headers(config)
        storage_record_id = urllib.parse.quote(id)
        response = requests.delete(
            STORAGE_URL + "/" + storage_record_id, headers=headers)

        if response.status_code in [204, 404]:
            logger.debug(
                f"Response to delete Record Id: {id} is {response.status_code}")
            success.append(id)
        else:
            logger.warning(f"Failed to delete Record Id: {id}")
            logger.error(f"Delete record response: {response}")
            failed.append(id)

    return success, failed


def verify_ids(record_ids):
    success = []
    failed = []
    search_query = create_search_query(record_ids)
    logger.debug(f"search query {search_query}")

    headers = get_headers(config)
    response = requests.post(SEARCH_URL, json.dumps(search_query),
                             headers=headers)

    if response.status_code in DATA_LOAD_OK_RESPONSE_CODES:
        search_response = response.json()
        logger.debug(f"search response {search_response}")
        ingested_records = search_response.get("results")

        for ingested_record in ingested_records:
            success.append(ingested_record.get("id"))

    failed = [x for x in record_ids if x not in success]
    if len(failed) > 0:
        logger.error(
            f"Failed to ingest Records {len(failed)} with Ids: {failed}")

    return success, failed


def create_search_query(record_ids):
    final_query = " OR ".join("id:\"" + x + "\"" for x in record_ids)
    return {
        "kind": "*:*:*:*.*.*",
        "returnedFields": ["id"],
        "offset": 0,
        "query": final_query,
        "limit": len(record_ids)
    }


def populate_file_metadata(file_source, file_name, description):
    file_type = file_name.split(".")[-1].upper()
    if file_type == "LAS":
        file_type = "LAS2"

    return {
        "kind": "osdu:wks:dataset--File.Generic:1.0.0",
        "acl": {
            "viewers": [
                config.get("REQUEST", "acl_viewer")
            ],
            "owners": [
                config.get("REQUEST", "acl_owner")
            ]
        },
        "legal": {
            "legaltags": [
                LEGAL_TAG
            ],
            "otherRelevantDataCountries": [
                "US"
            ],
            "status": "compliant"
        },
        "data": {
            "Description": description,
            "SchemaFormatTypeID": f"osdu:reference-data--SchemaFormatType:{file_type}:",
            "DatasetProperties": {
                "FileSourceInfo": {
                    "FileSource": file_source,
                    "Name": file_name
                }
            },
            "Name": file_name
        }
    }


def load_single_file(session, root, file):
    filepath = os.path.join(root, file)
    headers = get_headers(config)
    dir_name = root.split("/")[-1]

    logger.info(f"Starting upload for file: {filepath}")

    try:
        #####################
        # Get FileUrl
        #####################
        logger.debug("Requesting upload URL")
        response = session.get(FILE_URL + "/files/uploadURL", json={}, headers=headers)
        logger.debug(f"Upload URL response status: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"/files/uploadURL failed for {filepath} with response {response}")
            return FILE_UPLOAD_FAILED, filepath

        upload_url_response = response.json()
        signed_url = upload_url_response.get("Location").get("SignedURL")
        file_source = upload_url_response.get("Location").get("FileSource")
        logger.debug("Received signed URL")

        #####################
        # Put BLOB Data
        #####################
        loggeer.debug(f"Getting blob client")
        blob_client = BlobClient.from_blob_url(signed_url, max_single_put_size=MAX_CHUNK_SIZE * 1024)
        loggeer.debug(f"Opening file stream for {filepath}")
        with open(filepath, "rb") as file_stream:
            logger.debug(f"Uploading file to blob: {filepath}")
            upload_response = blob_client.upload_blob(file_stream, blob_type="BlockBlob", overwrite=True)
            logger.debug(f"Blob upload response: {upload_response}")

        #####################
        # Post MetaData
        #####################
        logger.debug("Populating metadata")
        metadata_body = json.dumps(populate_file_metadata(file_source, file, dir_name))
        logger.info(f"Posting request for file {filepath}: {metadata_body}")
        metadata_response = session.post(FILE_URL + "/files/metadata", metadata_body, headers=headers)
        logger.debug(f"Metadata response status: {metadata_response.status_code}")
        

        if metadata_response.status_code != 201:
            logger.error(f"/files/metadata failed for {filepath} with response {metadata_response.status_code} and body {metadata_response.text}")
            return FILE_UPLOAD_FAILED, filepath

        #####################
        # Get Record Version
        #####################
        file_id = metadata_response.json().get("id")
        logger.debug(f"Getting record version for file ID: {file_id}")
        version_response = session.get(STORAGE_URL + "/versions/" + file_id, headers=headers)
        logger.debug(f"Version response status: {version_response.status_code}")

        if version_response.status_code != 200:
            logger.error(f"/storage/versions failed for {file_id} with response {version_response}")
            return FILE_UPLOAD_FAILED, filepath

        record_version = version_response.json().get("versions")[0]
        logger.debug(f"Record version: {record_version}")

        output = {
            "file_id": file_id,
            "file_source": file_source,
            "file_record_version": str(record_version),
            "Description": dir_name
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"File Upload Failed: {file} Reason: {e}")
        return FILE_UPLOAD_FAILED, filepath

    logger.info(f"File Upload Completed: {file}")
    return FILE_UPLOAD_SUCCESS, (file, output)

def load_files(dir_name):
    logger.info("Starting load_files function")

    n_cores = multiprocessing.cpu_count()
    logger.info(f"Number of CPU cores: {n_cores}")

    if os.getenv("WORKERS") is not None:
        n_jobs = int(os.getenv("WORKERS"))
        logger.info(f"Using WORKERS environment variable for number of jobs: {n_jobs}")
    else:
        n_jobs = 2 * n_cores
        logger.info(f"Calculated number of jobs: {n_jobs}")

    dir_size = get_size_format(get_directory_size(dir_name))
    logger.info(f"Amount of data to transfer: {dir_size}")
    logger.info(f"Request worker count: {n_jobs}")
    logger.info(f"Max Chunk size: {MAX_CHUNK_SIZE} MB")

    results = []

    sessions = [requests_retry_session() for i in range(n_jobs)]
    logger.info(f"Created {n_jobs} sessions")

    for root, _, files in os.walk(dir_name):
        logger.info(f"Processing directory: {root}")
        files = [f for f in files if re.match(includes, f)]
        logger.info(f"Total number of files to upload: {len(files)}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_jobs) as executor:
            logger.info(f"Submitting {len(files)} files to the executor")
            future_result = {executor.submit(
                load_single_file, sessions[i % n_jobs], root, files[i]): i for i in range(0, len(files))}
            logger.info(f"Submitted {len(files)} files to the executor")
            
            for future in concurrent.futures.as_completed(future_result):
                file_index = future_result[future]
                try:
                    result = future.result()
                    if result[0] == FILE_UPLOAD_FAILED:
                        file_path = result[1]
                        logger.error(f"File upload failed for {file_path}")
                    else:
                        file_path, metadata = result[1]
                        logger.info(f"File upload succeeded for {file_path}")
                        logger.debug(f"Metadata for {file_path}: {metadata}")
                    results.append(result)
                except Exception as e:
                    logger.error(f"Exception occurred for file index {file_index}, Exception: {e}")
    
    failed = []
    success = {}
    for result in results:
        if result[0] == FILE_UPLOAD_FAILED:
            failed.append(result[1])
            logger.error(f"File upload failed: {result[1]}")
        else:
            file, metadata = result[1]
            success[file] = metadata
            logger.info(f"File upload succeeded: {file}")

    logger.info("Completed load_files function")
    return success, failed


def execute_sequence_ingestion(dir_name, batch_size, ingestion_sequence):
    with open(ingestion_sequence) as file:
        sequence = json.load(file)

    for entry in sequence:
        file_name = entry.get("FileName")
        filepath = os.path.join(dir_name, file_name)
        filepath_normalized = os.path.normpath(filepath)
        logger.debug(f"File to be ingested - {filepath_normalized}")

        object_to_ingest, data_type = get_object_to_ingest({}, True, filepath_normalized)
        if object_to_ingest is None:
            logger.warning(f"No objects to ingest for file: {filepath_normalized}")
            continue

        logger.debug(f"Ingesting objects from file: {filepath_normalized}")
        manifest_ingest(False, batch_size, object_to_ingest, data_type)

def execute_ingestion(dir_name, batch_size, is_wpc=False, file_location_map="", standard_reference=False): #kym
    for root, _, files in os.walk(dir_name):
        logger.debug(f"Files list: {files}")
        for file in files:
            data_objects = []
            filepath = os.path.join(root, file)
            object_to_ingest, data_type = get_object_to_ingest(file_location_map, standard_reference, filepath) #kym

            if object_to_ingest is None:
                continue

            if is_wpc:
                manifest_obj = populate_manifest(object_to_ingest, data_type)
                data_objects.append(manifest_obj)
            else:
                data_objects.append(object_to_ingest)

            manifest_ingest(is_wpc, batch_size, data_objects, data_type) #kym


def get_object_to_ingest(file_location_map, standard_reference, filepath): #kym
    logger.debug(f"parsing file for ingestion - {filepath}")

    if filepath.endswith(".json"):
        with open(filepath) as file:
            manifest_file = json.load(file)

            # For work product manifests inject the proper data-partition-id
            if standard_reference:
                a_data_partition = config.get("CONNECTION", "data-partition-id")
                new_manifest = json.dumps(manifest_file).replace('{{NAMESPACE}}', a_data_partition)
                manifest_file = json.loads(new_manifest)

    else:
        return None, None

    if not manifest_file:
        logger.error(f"Error with file {filepath}. File is empty.")
    elif "ReferenceData" in manifest_file and len(manifest_file["ReferenceData"]) > 0:
        object_to_ingest = update_reference_data_metadata(
            manifest_file["ReferenceData"], standard_reference)
        data_type = "ReferenceData"
    elif "MasterData" in manifest_file and len(manifest_file["MasterData"]) > 0:
        object_to_ingest = add_metadata(manifest_file["MasterData"])
        data_type = "MasterData"
    elif "Data" in manifest_file:
        data_type = "Data"
        if file_location_map is None or len(file_location_map) == 0:
            raise Exception(
                'File Location Map file path is required for Work-Product data ingestion')
        object_to_ingest = update_work_products_metadata(manifest_file["Data"], file_location_map,
                                                         get_directory_name(filepath))
    else:
        object_to_ingest = None
        data_type = None
    return object_to_ingest, data_type


def get_directory_name(filepath):
    dir_name = os.path.basename(os.path.dirname(filepath))
    return urllib.parse.quote(dir_name)


# def manifest_ingest(is_wpc, batch_size, data_objects, data_type): #kym
#     batch_objects = []
#     logger.debug(f"Manifest Ingestion - Splitting data into batches - Full data set size {len(data_objects)}, splitting into batches of {batch_size} records")

#     for i, data_object in enumerate(data_objects):
#         batch_objects.append(data_object)

#         if len(batch_objects) == batch_size or i == len(data_objects) - 1:
#             if is_wpc:
#                 request_data = populate_workflow_request(batch_objects)
#                 logger.debug(f"Sending Request with WPC data, batch number: {len(batch_objects)}")
#             else:
#                 request_data = populate_typed_workflow_request(batch_objects, data_type)
#                 logger.debug(f"Sending Request with batch number: {len(batch_objects)}")

#             send_request(request_data)
#             batch_objects = []

def send_batch_request(batch_objects, is_wpc, data_type):
    if is_wpc:
        request_data = populate_workflow_request(batch_objects)
        logger.debug(f"Sending Request with WPC data, batch number: {len(batch_objects)}")
    else:
        request_data = populate_typed_workflow_request(batch_objects, data_type)
        logger.debug(f"Sending Request with batch number: {len(batch_objects)}")

    send_request(request_data)

def manifest_ingest(is_wpc, batch_size, data_objects, data_type): #kym
    batch_objects = []
    logger.debug(f"Manifest Ingestion - Splitting data into batches - Full data set size {len(data_objects)}, splitting into batches of {batch_size} records")
    logger.debug(f"Data type: {data_type}, WPC mode: {is_wpc}, data_objects: {data_objects}")

    for data_object in data_objects:
        if data_type == "MasterData":
            for record in data_object:  # Iterate over the records inside each data_object
                batch_objects.append(record)

                # Process batch when the size limit is reached
                if len(batch_objects) == batch_size:
                    send_batch_request(batch_objects, is_wpc, data_type)
                    batch_objects = []  # Reset for the next batch

        else:
            # Handle non-MasterData types (if they don't need special handling)
            batch_objects.append(data_object)

            if len(batch_objects) == batch_size or data_object == data_objects[-1]:
                send_batch_request(batch_objects, is_wpc, data_type)
                batch_objects = []  # Reset for the next batch

    # Send any remaining records in the final batch
    if batch_objects:
        send_batch_request(batch_objects, is_wpc, data_type)


def status_check():
    with open(LOG_FILENAME) as f:
        run_id_list = [run_id.rstrip() for run_id in f]

    logger.debug(f"list of run-ids: {run_id_list}")

    results = []
    for run_id in run_id_list:
        workflow_status = send_status_check_request(run_id)
        if workflow_status is not None:
            status = workflow_status.get(STATUS)
            if status == "running":
                results.append({
                    RUN_ID: run_id,
                    STATUS: status})
            else:
                start_time = workflow_status.get(START_TIME)
                end_time = workflow_status.get(END_TIME)
                time_taken = -1000
                if start_time is not None and end_time is not None:
                    time_taken = end_time - start_time
                logger.info(f"Workflow {status}: {run_id}")
                results.append({
                    RUN_ID: run_id,
                    END_TIME: end_time,
                    START_TIME: start_time,
                    STATUS: status,
                    TIME_TAKEN: time_taken / 1000})
        else:
            results.append({
                RUN_ID: run_id,
                STATUS: "Unable To fetch status"})

    return results


def send_status_check_request(run_id):
    headers = get_headers(config)

    # send batch request for creating records
    response = requests.get(WORKFLOW_URL + "/" + run_id, headers=headers)

    if response.status_code in DATA_LOAD_OK_RESPONSE_CODES:
        workflow_response = response.json()
        logger.debug(f"Get status Response: {workflow_response}")
        return workflow_response
    else:
        reason = response.text[:250]
        logger.error(f"Request error for {headers.get('correlation-id')}")
        logger.error(f"Response status: {response.status_code}. "
                     f"Response content: {reason}.")
        return None


def update_work_products_metadata(data, file_location_map, base_dir):
    reference_pattern = "{}:reference-data".format(
        config.get("CONNECTION", "data-partition-id"))
    master_pattern = "{}:master-data".format(
        config.get("CONNECTION", "data-partition-id"))

    updated_manifest = json.dumps(data).replace(
        'osdu:reference-data', reference_pattern).replace(
        'osdu:master-data', master_pattern).replace(
        "surrogate-key:file-1", "surrogate-key:dataset--1:0:0").replace(
        "surrogate-key:wpc-1", "surrogate-key:wpc--1:0:0")

    data = json.loads(updated_manifest)

    logger.debug(f"Base directory is {base_dir}")
    update_legal_and_acl_tags(data["WorkProduct"])
    add_metadata(data["WorkProductComponents"])
    add_metadata(data["Datasets"])

    with open(file_location_map) as file:
        location_map = json.load(file)

    file_name = data["WorkProduct"]["data"]["Name"]

    if file_name in location_map:
        file_source = location_map[file_name]["file_source"]
        file_id = location_map[file_name]["file_id"]
        file_version = location_map[file_name]["file_record_version"]

        # Update Dataset with Generated File Id and File Source.
        data["Datasets"][0]["id"] = file_id
        data["Datasets"][0]["data"]["DatasetProperties"]["FileSourceInfo"]["FileSource"] = file_source
        del data["Datasets"][0]["data"]["DatasetProperties"]["FileSourceInfo"]["PreloadFilePath"]

        # Update FileId in WorkProductComponent
        data["WorkProductComponents"][0]["data"]["Datasets"][0] = file_id + \
            ":" + file_version

        # Todo: remove this if not required later.
        if id not in data["WorkProduct"]:
            # Add the Work-Product Id -> opendes:work-product--WorkProduct:load_document_69_D_CH_11_pdf.json
            data["WorkProduct"]["id"] = generate_workproduct_id(
                file_name, base_dir)
    else:
        logger.warning(f"File {file_name} does not exist")

    logger.debug(f"data to upload workproduct \n {data}")
    return data


def generate_workproduct_id(file_name, base_dir):
    return "{}:work-product--WorkProduct:{}-{}".format(config.get("CONNECTION", "data-partition-id"),
                                                       base_dir, file_name)


def reference_data_id(datum, reference_pattern = None, master_pattern = None):
    string = str.replace(datum["id"], "{{NAMESPACE}}", config.get("CONNECTION", "data-partition-id"))
    if reference_pattern is not None:
        string = string.replace('osdu:reference-data', reference_pattern)
    if master_pattern is not None:    
        string = string.replace('osdu:master-data', master_pattern)
    return string


def update_reference_data_metadata(data, is_standard_reference):
    reference_pattern = "{}:reference-data".format(config.get("CONNECTION", "data-partition-id"))
    master_pattern = "{}:master-data".format(config.get("CONNECTION", "data-partition-id"))
    for datum in data:
        if "id" in datum:
            datum["id"] = reference_data_id(datum, reference_pattern, master_pattern)
        update_legal_and_acl_tags(datum)
    return data


def add_metadata(data):
    reference_pattern = "{}:reference-data".format(config.get("CONNECTION", "data-partition-id"))
    master_pattern = "{}:master-data".format(config.get("CONNECTION", "data-partition-id"))
    for datum in data:
        if "id" in datum:
            datum["id"] = datum["id"].replace('osdu:reference-data', reference_pattern).replace('osdu:master-data', master_pattern)
        update_legal_and_acl_tags(datum)
    return data


def update_legal_and_acl_tags(datum):
    datum["legal"]["legaltags"] = [LEGAL_TAG]
    datum["legal"]["otherRelevantDataCountries"] = ["US"]
    datum["acl"]["viewers"] = [config.get("REQUEST", "acl_viewer")]
    datum["acl"]["owners"] = [config.get("REQUEST", "acl_owner")]


def populate_workflow_request(manifest):
    request = {
        "executionContext": {
            "Payload": {
                "AppKey": "test-app",
                "data-partition-id": config.get("CONNECTION", "data-partition-id")
            },
            "manifest": manifest
        }
    }
    logger.debug(f"Request to be sent {request}")
    return request


def populate_typed_workflow_request(data, data_type):
    request = {
        "executionContext": {
            "Payload": {
                "AppKey": "test-app",
                "data-partition-id": config.get("CONNECTION", "data-partition-id")
            },
            "manifest": populate_manifest(data, data_type)
        }
    }
    logger.debug(f"Request to be sent {request}") #####
    return request


def populate_manifest(data, data_type):
    return {
        "kind": "osdu:wks:Manifest:1.0.0",
                data_type: data
    }


def send_request(request_data):
    # loop for implementing retries send process
    retries = config.getint("CONNECTION", "retries")
    for retry in range(retries):
        try:
            headers = get_headers(config)
            # send batch request for creating records
            response = requests.post(WORKFLOW_URL, json.dumps(request_data),
                                     headers=headers)

            if response.status_code in DATA_LOAD_OK_RESPONSE_CODES:
                workflow_response = response.json()
                logger.debug(f"Response: {workflow_response}")
                logger.info(f"Workflow Submitted: {workflow_response.get('runId')}")
                file_logger.info(f"{workflow_response.get('runId')}")
                break

            reason = response.text[:250]
            logger.error(f"Request error.")
            logger.error(f"Response status: {response.status_code}. "
                         f"Response content: {reason}.")

            if retry + 1 < retries:
                if response.status_code in BAD_TOKEN_RESPONSE_CODES:
                    logger.error(
                        f"Error in Request: {headers.get('correlation-id')})")
                else:
                    time_to_sleep = config.getint("CONNECTION", "timeout")

                    logger.info(f"Retrying in {time_to_sleep} seconds...")
                    time.sleep(time_to_sleep)

        except (requests.RequestException, HTTPError) as exc:
            logger.error(f"Unexpected request error. Reason: {exc}")
            sys.exit(2)


def compute_reports(reports_file):
    with open(reports_file) as file:
        ingestion_results = json.load(file)
    computed_baseline = []

    for ingestion_name in ingestion_results:
        part_results = ingestion_results.get(ingestion_name)

        success = []
        failed = []
        incomplete = []

        for result in part_results:
            if result.get(STATUS) == FINISHED:
                success.append(result)
            elif result.get(STATUS) == FAILED:
                failed.append(result)
            else:
                incomplete.append(result)

        logger.info(f"Ingestion results {success}")

        average_time_taken = sum([s.get(TIME_TAKEN)
                                 for s in success]) / len(success)

        min_start_time = min([s.get(START_TIME) for s in success])
        max_end_time = max([s.get(END_TIME) for s in success])

        computed_baseline.append({
            "data_type": ingestion_name,
            "time_taken_in_minutes": (max_end_time - min_start_time) / (60 * 1000),
            "ingestion_runs_successful": len(success),
            "ingestion_runs_failed": len(failed),
            "ingestion_runs_incomplete": len(incomplete),
            "avg_time_taken_per_dag_run_in_seconds": average_time_taken
        })

    with open('generated_reports.json', 'w') as f:
        json.dump(computed_baseline, f, indent=4)
        logger.info(f"Reports are generated and saved to {f.name}")


def main(argv):
    # Arg Parser - https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subparser')

    # Ingest command
    parser_ingest = subparsers.add_parser('ingest')
    parser_ingest.add_argument('-d', '--dir', dest='dir', help='Directory name', required=True)
    parser_ingest.add_argument('-b', '--batch', dest='batch', help='Batch size', required=False, type=int, default=1)
    parser_ingest.add_argument('-w', "--work-products", dest="wpc", help="Is workproduct data?", action='store_true', required=False, default=False)
    parser_ingest.add_argument('-f', '--file-location-map-file', dest='file_location_map', help='Json file where file locations are stored', required=False)
    parser_ingest.add_argument('-r', '--standard-reference', dest='standard_reference', help="Is standard reference data?", default=False, action='store_true')

    # Standard references
    parser_references = subparsers.add_parser("references")
    parser_references.add_argument('-d', '--dir', dest='dir', help='Directory name', required=True)
    parser_references.add_argument('-s', '--ingestion-sequence-file', dest='ingestion_sequence', help='Json file where file locations are stored', required=True)

    # Status command
    parser_status = subparsers.add_parser('status')
    parser_status.add_argument('-w', '--wait', dest='wait', help='Should wait for ingestion to complete', action='store_true', required=False, default=False)
    parser_status.add_argument("-i", "--ingestion-name", dest="ingestion_name", default="ingestion")

    # Reports command
    parser_reports = subparsers.add_parser('reports')
    parser_reports.add_argument('-f', '--file', dest="file", help="Reports File", required=True)

    # Verify command
    parser_verify = subparsers.add_parser('verify')
    parser_verify.add_argument('-d', '--dir', dest='dir', help='Directory name', required=True)
    parser_verify.add_argument('-b', '--batch', dest='batch', help='Batch size', required=False, type=int, default=1)
    parser_verify.add_argument('-r', '--standard-reference', dest='standard_reference', help="Is standard reference data?", default=False, action='store_true')
    parser_verify.add_argument('-s', '--ingestion-sequence-file', dest='ingestion_sequence', help='Json file where file locations are stored', required=False)

    # Delete command
    parser_verify = subparsers.add_parser('delete')
    parser_verify.add_argument('-d', '--dir', dest='dir', help='Directory name', required=True)

    # Datasets command
    parser_verify = subparsers.add_parser('datasets')
    parser_verify.add_argument('-d', '--dir', dest='dir', help='Directory name', required=True)
    parser_verify.add_argument( "-f", "--output-file-name", dest="output", help="File to which the file info is saved to", default="datasets-location.json")

    parsed_args = parser.parse_args()
    logger.info(f"args: {parsed_args}")


    #####################
    # Action: ingest
    #####################
    if parsed_args.subparser == "ingest":
        open(LOG_FILENAME, 'w').close()

        # Get Arguments
        vars_parsed_args = vars(parsed_args)
        a_dir = vars_parsed_args.get("dir")
        a_batch_size = vars_parsed_args.get("batch")
        a_is_wpc = vars_parsed_args.get("wpc")
        a_location_map = vars_parsed_args.get("file_location_map")
        a_standard_ref = vars_parsed_args.get("standard_reference")

        # Execute Action
        execute_ingestion(a_dir, a_batch_size, a_is_wpc, a_location_map, a_standard_ref) #kym

    #####################
    # Action: references
    #####################
    elif parsed_args.subparser == "references":
        open(LOG_FILENAME, 'w').close()

        # Get Arguments
        vars_parsed_args = vars(parsed_args)
        a_dir = vars_parsed_args.get("dir")
        a_sequence = vars_parsed_args.get("ingestion_sequence")
        a_batch_size = vars_parsed_args.get("batch")

        # Execute Action
        execute_sequence_ingestion(a_dir, a_batch_size, ingestion_sequence=a_sequence)

    #####################
    # Action: datasets
    #####################
    elif parsed_args.subparser == "datasets":

        # Get Arguments
        vars_parsed_args = vars(parsed_args)
        a_dir = vars_parsed_args.get("dir")
        a_file_name = vars_parsed_args.get("output")

        # Execute Action
        success, failed = load_files(a_dir)
        logger.debug(f"Files that are successfully uploaded: {len(success)}")
        # Create Result File
        with open(a_file_name, 'w') as f:
            json.dump(success, f, indent=4)
            logger.info(f"File location map is saved to {f.name}")
        logger.info(f"Files that could not be uploaded: {len(failed)}")
        logger.info(pformat(failed))

    #####################
    # Action: status
    #####################
    elif parsed_args.subparser == "status":

        # Get Arguments
        vars_parsed_args = vars(parsed_args)
        a_wait = vars_parsed_args.get("wait")
        a_run_id = vars_parsed_args.get("ingestion_name")

        # Execute Action
        results = status_check()

        if a_wait:
            while True:
                completed = True
                for result in results:
                    if result.get("status") == "running":
                        completed = False
                        break

                if completed:
                    break

                time.sleep(60)

                # Execute Action
                results = status_check()

        if not os.path.isfile(RESULTS_FILENAME):
            # checks if file exists
            results_map = {}
        else:
            # Load existing manifest ingestion results
            with open(RESULTS_FILENAME) as file:
                results_map = json.load(file)

        results_map[a_run_id] = results

        with open(RESULTS_FILENAME, 'w') as f:
            json.dump(results_map, f, indent=4)


    #####################
    # Action: verify
    #####################
    elif parsed_args.subparser == "verify":
        # Verify the ingestion is successful
        vars_parsed_args = vars(parsed_args)
        if vars_parsed_args.get("standard_reference"):
            logger.info("Verifying standard references")
            success, failed = verify_references(
                vars_parsed_args.get("dir"),
                ingestion_sequence=vars_parsed_args.get("ingestion_sequence"))
            pass
        else:
            success, failed = verify_ingestion(
                vars_parsed_args.get("dir"), vars_parsed_args.get("batch"))
        logger.info(
            f"Number of Records that are ingested successfully: {len(success)}")
        logger.warning(
            f"Number of Records that could not be ingested successfully: {len(failed)}")

        if len(failed) > 0:
            logger.info("Record IDs that could not be ingested")
            logger.info(pformat(failed))

    #####################
    # Action: reports
    #####################
    elif parsed_args.subparser == "reports":
        vars_parsed_args = vars(parsed_args)
        compute_reports(vars_parsed_args.get("file"))

    #####################
    # Action: delete
    #####################
    elif parsed_args.subparser == "delete":
        # Delete the list of records
        success, failed = delete_ingested_records(vars(parsed_args).get("dir"))
        logger.info(
            f"Number of Records that are deleted successfully: {len(success)}")
        logger.info("Record IDs that could not be deleted")
        logger.info(pformat(failed))



if __name__ == "__main__":
    main(sys.argv[1:])
