#!/usr/bin/env bash
#  Purpose: Cordinate and Execute all Loading Activities.
#  Version: 0.1
#  Usage:
#    load.sh

usage() { echo "Usage: load.sh <data_partition>" 1>&2; exit 1; }

if [ -z $LOG_LEVEL ]; then LOG_LEVEL="info"; fi
if [ -z $BATCH_SIZE ]; then BATCH_SIZE=100; fi
if [ -z $PIP_INSTALL ]; then PIP_INSTALL=true; else if [ $PIP_INSTALL != false ]; then PIP_INSTALL=true; fi fi
if [ -z $CONFIGURE_INI ]; then CONFIGURE_INI=true; else if [ $CONFIGURE_INI != false ]; then CONFIGURE_INI=true; fi fi
if [ -z $CHECK_LEGAL_TAG ]; then CHECK_LEGAL_TAG=true; else if [ $CHECK_LEGAL_TAG != false ]; then CHECK_LEGAL_TAG=true; fi fi
if [ -z $GENERATE_MANIFEST ]; then GENERATE_MANIFEST=true; else if [ $GENERATE_MANIFEST != false ]; then GENERATE_MANIFEST=true; fi fi
if [ -z $LOAD_MASTERDATA ]; then LOAD_MASTERDATA=true; else if [ $LOAD_MASTERDATA != false ]; then LOAD_MASTERDATA=true; fi fi
if [ -z $LOAD_FILES ]; then LOAD_FILES=true; else if [ $LOAD_FILES != false ]; then LOAD_FILES=true; fi fi
if [ -z $LOAD_WORKPRODUCTS ]; then LOAD_WORKPRODUCTS=true; else if [ $LOAD_WORKPRODUCTS != false ]; then LOAD_WORKPRODUCTS=true; fi fi
if [ -z $KEEP_OPEN ]; then KEEP_OPEN=false; fi

if [ -z $1 ]; then
  if [ -z $DATA_PARTITION ]; then
    tput setaf 1; echo "ERROR: Argument \$1 - Data Partition name required." ; tput sgr0
    usage;
  fi
else
  DATA_PARTITION=$1
fi

if [ -z $DATA_DOMAIN ]; then DATA_DOMAIN='contoso.com'; fi

while getopts p: flag
do
    case "${flag}" in
        p) data_partition=${OPTARG};;
    esac
done

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PARENT_DIR=`dirname $SCRIPT_DIR`
MANIFEST_DIR="manifests"

###############################
## FUNCTIONS                 ##
###############################
convertsecs() {
 ((h=${1}/3600))
 ((m=(${1}%3600)/60))
 ((s=${1}%60))
 printf "%02d:%02d:%02d\n" $h $m $s
}
function InstallRequirements() {
  if [ $PIP_INSTALL = true ]; then
    echo "PIP Install"
    echo "-----Start-----"
    pip install --user -r $SCRIPT_DIR/requirements.txt
    echo "-----End-----"
  else
    echo "PIP Install - Bypassed"
    echo "-----"
  fi
}
function ConfigureIni() {
  if [ $CONFIGURE_INI = true ]; then
    echo "Configuring dataload.ini"
    cp -f $SCRIPT_DIR/src/config/dataload.template.ini $SCRIPT_DIR/output/dataload.ini
    sed -i -e "s?<ACL_OWNER>?${ACL_OWNER}?g" "$SCRIPT_DIR/output/dataload.ini"
    sed -i -e "s?<ACL_VIEWER>?${ACL_VIEWER}?g" "$SCRIPT_DIR/output/dataload.ini"
    sed -i -e "s?<OSDU_ENDPOINT>?${OSDU_ENDPOINT}?g" "$SCRIPT_DIR/output/dataload.ini"
    sed -i -e "s/<LEGAL_TAG>/${LEGAL_TAG}/g" "$SCRIPT_DIR/output/dataload.ini"
    sed -i -e "s/<DATA_PARTITION>/${DATA_PARTITION}/g" "$SCRIPT_DIR/output/dataload.ini"
    sed -i -e "s?<LOGIN_ENDPOINT>?${LOGIN_ENDPOINT}?g" "$SCRIPT_DIR/output/dataload.ini"
    sed -i -e "s?<DOMAIN>?${DATA_DOMAIN}?g" "$SCRIPT_DIR/output/dataload.ini"
    echo "-----"
  else
    echo "Config dataload.ini - Bypassed."
    echo "-----"
  fi
}
function ValidateLegalTag() {
  if [ $CHECK_LEGAL_TAG = true ]; then
    echo "Checking Legal Tag"
    echo "-----Start-----"
    python3 $SCRIPT_DIR/src/data_load/osdu.py
    echo "-----End-----"
  else
    echo "Check LegalTag - Bypassed."
    echo "-----"
  fi
}
function GenerateManifests() {
  if [ $GENERATE_MANIFEST = true ]; then
    echo "-- Generate Manifests: Start" && _START="$(date +%s)"
    rm -rf $MANIFEST_DIR 2> /dev/null || true

    # Install Manifest Load Python Module
    python3 setup.py install --user

    # Generating reference data manifests
    bash $SCRIPT_DIR/src/generator.sh \
      -m "$SCRIPT_DIR/src/config/tno_ref_data_template_mapping.json" \
      -t "reference_data" \
      -d "reference-data" \
      -o "${MANIFEST_DIR}/reference-manifests" \
      -g true -p "${DATA_PARTITION}"

    # Generating misc master data manifests
    bash $SCRIPT_DIR/src/generator.sh \
      -m "$SCRIPT_DIR/src/config/tno_misc_master_data_template_mapping.json" \
      -t "master_data" \
      -d "master-data/Misc_master_data" \
      -o "${MANIFEST_DIR}/misc-master-data-manifests" \
      -g true -p "${DATA_PARTITION}"

    # Generating master well data manifests
    bash $SCRIPT_DIR/src/generator.sh \
      -m "$SCRIPT_DIR/src/config/tno_well_data_template_mapping.json" \
      -t "master_data" \
      -d "master-data/Well" \
      -o "${MANIFEST_DIR}/master-well-data-manifests" \
      -p "${DATA_PARTITION}"

    # Generating master wellbore data manifests
    bash $SCRIPT_DIR/src/generator.sh \
      -m "$SCRIPT_DIR/src/config/tno_wellbore_data_template_mapping.json" \
      -t "master_data" \
      -d "master-data/Wellbore" \
      -o "${MANIFEST_DIR}/master-wellbore-data-manifests" \
      -p "${DATA_PARTITION}"

    echo "-- Generate Manifest: End  $(convertsecs $[ $(date +%s) - ${_START} ])"
  else
    echo "Manifest Generation - Bypassed"
    echo "-----"
  fi
}
function LoadMasterData() {
  if [ $LOAD_MASTERDATA = true ]; then
    echo "Loading Master Data"

    # Manifest Ingest Reference
    echo "-- Reference Data: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --dir $MANIFEST_DIR/reference-manifests \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait
    echo "-- Reference Data: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # Manifest Ingest Misc Master
    echo "-- Misc Master Data: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --dir $MANIFEST_DIR/misc-master-data-manifests \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait
    echo "-- Misc Master Data: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # Manifest Ingest Master Well
    echo "-- Master Well Data: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --dir $MANIFEST_DIR/master-well-data-manifests \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait
    echo "-- Master Well Data: End  $(convertsecs $[ $(date +%s) - ${_START} ])"
  else
    echo "Load Master Data - Bypassed"
    echo "-----"
  fi
}
function LoadFiles() {
  if [ $LOAD_FILES = true ]; then
    echo "Loading File Data"
    rm $SCRIPT_DIR/output/*.json 2> /dev/null || true

    # File Ingest Documents
    echo "-- WPC Documents: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py datasets \
      --dir $SCRIPT_DIR/open-test-data/datasets/documents \
      --output-file-name "output/loaded-documents-datasets.json"
    echo "-- WPC Documents: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # File Ingest Well Logs
    echo "-- WPC WellLogs: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py datasets \
      --dir $SCRIPT_DIR/open-test-data/datasets/well-logs \
      --output-file-name "output/loaded-welllogs-datasets.json"
    echo "-- WPC WellLogs: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # File Ingest Markers
    echo "-- WPC Markers: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py datasets \
      --dir $SCRIPT_DIR/open-test-data/datasets/markers \
      --output-file-name "output/loaded-marker-datasets.json"
    echo "-- WPC Markers: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # File Ingest Trajectories
    echo "-- WPC Trajectories: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py datasets \
      --dir $SCRIPT_DIR/open-test-data/datasets/trajectories \
      --output-file-name "output/loaded-trajectories-datasets.json"
    echo "-- WPC Trajectories: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

  else
    echo "Load Files - Bypassed"
    echo "-----"
  fi
}
function LoadWorkProducts() {
  if [ $LOAD_WORKPRODUCTS = true ]; then
    echo "Loading Work Product Manifests"

    # Manifest Ingest Markers
    echo "-- Markers: Start" && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --work-products \
      --file-location-map-file "output/loaded-marker-datasets.json" \
      --dir $SCRIPT_DIR/open-test-data/TNO/provided/TNO/work-products/markers \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait --ingestion-name "markers"
    echo "-- Markers: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # Manifest Ingest Trajectories
    echo "-- Trajectories: Start"  && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --work-products \
      --file-location-map-file "output/loaded-trajectories-datasets.json" \
      --dir $SCRIPT_DIR/open-test-data/TNO/provided/TNO/work-products/trajectories \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait --ingestion-name "trajectories"
    echo "-- Trajectories: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # Manifest Ingest Well Logs
    echo "-- Well Logs: Start"  && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --work-products \
      --file-location-map-file "output/loaded-welllogs-datasets.json" \
      --dir $SCRIPT_DIR/open-test-data/TNO/provided/TNO/work-products/well\ logs \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait --ingestion-name "well-logs"
    echo "-- Well Logs: End  $(convertsecs $[ $(date +%s) - ${_START} ])"

    # Manifest Ingest Documents
    echo "-- Documents: Start"  && _START="$(date +%s)"
    python3 $SCRIPT_DIR/src/data_load/load.py ingest \
      --work-products \
      --file-location-map-file "output/loaded-documents-datasets.json" \
      --dir $SCRIPT_DIR/open-test-data/TNO/provided/TNO/work-products/documents \
      --batch $BATCH_SIZE
    python3 $SCRIPT_DIR/src/data_load/load.py status --wait --ingestion-name "documents"
    echo "-- Documents: End  $(convertsecs $[ $(date +%s) - ${_START} ])"
  else
    echo "Load Work Product Manifests - Bypassed"
    echo "-----"
  fi
}


###############################
## EXECUTION                 ##
###############################
printf "\n"
echo "=================================================================="
echo "Initializing"
echo "=================================================================="
rm $SCRIPT_DIR/output/*.log 2> /dev/null || true
InstallRequirements;
ConfigureIni;
ValidateLegalTag;


printf "\n"
START1="$(date +%s)"
echo "=================================================================="
echo "Load Dataset Files"
echo "=================================================================="
LoadFiles;


printf "\n"
START2="$(date +%s)"
echo "=================================================================="
echo "Generating Manifests"
echo "=================================================================="
GenerateManifests;


printf "\n"
START3="$(date +%s)"
echo "=================================================================="
echo "Load Reference and Master Data Manifests"
echo "=================================================================="
LoadMasterData;


printf "\n"
START4="$(date +%s)"
echo "=================================================================="
echo "Load Work Products"
echo "=================================================================="
LoadWorkProducts;


printf "\n"
echo "=================================================================="
echo "Execution Time Report - (Seconds)"
echo "=================================================================="
echo "Load Files:  $(convertsecs $[ ${START2} - ${START1} ])"
echo "Generating Manifests:  $(convertsecs $[ ${START3} - ${START2} ])"
echo "Load Reference and Master Data Manifests:  $(convertsecs $[ ${START4} - ${START3} ])"
echo "Load Work Product Manifests:  $(convertsecs $[ $(date +%s) - ${START4} ])"

# Debugging Loop
if [ $KEEP_OPEN = true ]; then
  echo "=================================================================="
  echo "Enter Container Debug Loop"
  echo "=================================================================="
  while true; do sleep 2; done
fi