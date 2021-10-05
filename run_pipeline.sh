#!/usr/bin/env bash

set -e


if [[ $# -ne 5 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <pipeline-name> <google-cloud-credentials-file-path> <configuration-module> <output-dir>"
    echo "Runs the pipeline end-to-end (sync-rapid-pro-to-engagement-db, sync-engagement-db-to-coda, sync-coda-to-engagement-db,\
          sync-engagement-db-to-rapid-pro, run-engagement-db-to-analysis,)"
    exit
fi

USER=$1
PIPELINE_NAME=$2
GOOGLE_CLOUD_CREDENTIALS_PATH=$3
CONFIGURATION_MODULE=$4
OUTPUT_DIR=$5

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"

./docker-run-log-pipeline-event.sh  "$CONFIGURATION_MODULE" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$RUN_ID" "PipelineRunStart"

./docker-sync-rapid-pro-to-engagement-db.sh --incremental-cache-volume "$PIPELINE_NAME-rapid-pro-to-engagement-db-cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                                    "$CONFIGURATION_MODULE"

./docker-sync-engagement-db-to-coda.sh --incremental-cache-volume "$PIPELINE_NAME-engagement-db-to-coda-cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                        "$CONFIGURATION_MODULE"

./docker-sync-coda-to-engagement-db.sh --incremental-cache-volume "$PIPELINE_NAME-coda-to-engagement-db-cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                        "$CONFIGURATION_MODULE"

./docker-run-engagement-db-to-analysis.sh --incremental-cache-volume "$PIPELINE_NAME-engagement-db-to-analysis-cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                        "$CONFIGURATION_MODULE" "$OUTPUT_DIR"

./docker-run-log-pipeline-event.sh  "$CONFIGURATION_MODULE" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$RUN_ID" "PipelineRunEnd"
