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

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PARENT_DIR=`dirname $SCRIPT_DIR`
FILE_NAME=open-test-data.gz

cd $PARENT_DIR

if [ ! -d "$PARENT_DIR/open-test-data" ]
then
    wget -O $FILE_NAME https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data/-/archive/v0.26.0/open-test-data-v0.26.0.tar.gz

    # Copy File Data
    mkdir -p $PARENT_DIR/open-test-data/datasets/documents && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/datasets/documents --strip-components=5 open-test-data-v0.26.0/rc--1.0.0/1-data/3-provided/USGS_docs
    mkdir -p $PARENT_DIR/open-test-data/datasets/markers && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/datasets/markers --strip-components=5 open-test-data-v0.26.0/rc--1.0.0/1-data/3-provided/markers
    mkdir -p $PARENT_DIR/open-test-data/datasets/trajectories && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/datasets/trajectories --strip-components=5 open-test-data-v0.26.0/rc--1.0.0/1-data/3-provided/trajectories
    mkdir -p $PARENT_DIR/open-test-data/datasets/well-logs && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/datasets/well-logs --strip-components=5 open-test-data-v0.26.0/rc--1.0.0/1-data/3-provided/well-logs

    # Copy Manifest Data
    mkdir -p $PARENT_DIR/open-test-data/schema && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/schema --strip-components=3 open-test-data-v0.26.0/rc--3.0.0/3-schema
    mkdir -p $PARENT_DIR/open-test-data/templates && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/templates --strip-components=3 open-test-data-v0.26.0/rc--3.0.0/5-templates
    mkdir -p $PARENT_DIR/open-test-data/TNO/contrib && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/TNO/contrib --strip-components=5 open-test-data-v0.26.0/rc--3.0.0/1-data/3-provided/TNO
    mkdir -p $PARENT_DIR/open-test-data/TNO/provided && \
        tar -xzvf $FILE_NAME -C $PARENT_DIR/open-test-data/TNO/provided --strip-components=3 open-test-data-v0.26.0/rc--3.0.0/4-instances/TNO

    rm $FILE_NAME
fi
