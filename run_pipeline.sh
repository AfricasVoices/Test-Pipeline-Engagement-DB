#!/usr/bin/env bash

set -e


if [[ $# -ne 6 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <pipeline-name> <google-cloud-credentials-file-path> <configuration-module> <data-dir>"
    echo "Runs the pipeline end-to-end (sync-rapid-pro-to-engagement-db, sync-engagement-db-to-coda, sync-coda-to-engagement-db,\
          sync-engagement-db-to-rapid-pro, run-engagement-db-to-analysis, ARCHIVE)"
    exit
fi

USER=$1
PIPELINE_NAME=$2
GOOGLE_CLOUD_CREDENTIALS_PATH=$3
CONFIGURATION_MODULE=$4
DATA_DIR=$5
ARCHIVE_LOCATION=$6

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"
ARCHIVE_FILE="$ARCHIVE_LOCATION/data-$RUN_ID.tar.gzip"

./docker-run-engagement-db-to-analysis.sh --incremental-cache-volume "$PIPELINE_NAME-engagement-db-to-analysis-cache" \
                        "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_MODULE" "$DATA_DIR"