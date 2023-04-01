#!/usr/bin/env bash

set -e

if [[ $# -ne 7 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "<user> <pipeline-name> <google-cloud-credentials-file-path> <configuration-file> <code-schemes-dir> <data-dir> <archive-dir>"
    echo "Runs the pipeline end-to-end (sync-rapid-pro-to-engagement-db, sync-engagement-db-to-coda, sync-coda-to-engagement-db,\
          sync-engagement-db-to-rapid-pro, run-engagement-db-to-analysis, ARCHIVE)"
    exit
fi

USER=$1
PIPELINE_NAME=$2
GOOGLE_CLOUD_CREDENTIALS_PATH=$3
CONFIGURATION_FILE=$4
CODE_SCHEMES_DIR=$5
DATA_DIR=$6
ARCHIVE_DIR=$7

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"
ARCHIVE_FILE="$ARCHIVE_DIR/data-$RUN_ID.tar.gzip"

echo "Starting a new pipeline run with id ${RUN_ID}"

./docker-run-kobotoolbox-to-engagement-db.sh \
    --incremental-cache-volume "$PIPELINE_NAME-kobotoolbox-to-engagement-db-cache"  \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR" "$DATA_DIR"

./docker-sync-engagement-db-to-coda.sh \
    --incremental-cache-volume "$PIPELINE_NAME-engagement-db-to-coda-cache" \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR" "$DATA_DIR"

./docker-sync-coda-to-engagement-db.sh \
    --incremental-cache-volume "$PIPELINE_NAME-coda-to-engagement-db-cache" \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR" "$DATA_DIR"

./docker-sync-engagement-db-to-rapid-pro.sh \
    --incremental-cache-volume "$PIPELINE_NAME-engagement-db-to-rapid-pro-cache"  \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR"

./docker-run-engagement-db-to-analysis.sh \
    --incremental-cache-volume "$PIPELINE_NAME-engagement-db-to-analysis-cache" \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR" "$DATA_DIR"

./archive_data_dir.sh "$DATA_DIR" "$ARCHIVE_FILE"

./docker-run-upload-archive-files.sh \
    "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR" "$ARCHIVE_DIR"

./docker-run-log-pipeline-event.sh \
    "$CONFIGURATION_FILE" "$CODE_SCHEMES_DIR" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$RUN_ID" "PipelineRunEnd"
