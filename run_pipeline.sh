#!/usr/bin/env bash

set -e

if [[ $# -ne 4 ]]; then
    echo "Usage: ./run_pipeline.sh"
    echo "  <user> <google-cloud-credentials-file-path> <configuration-module> <output-dir>"
    echo "Runs the pipeline end-to-end (sync-rapid-pro-to-engagement-db, sync-engagement-db-to-coda, sync-coda-to-engagement-db,\
          sync-engagement-db-to-rapid-pro, run-engagement-db-to-analysis,)"
    exit
fi

USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
CONFIGURATION_MODULE=$3
OUTPUT_DIR=$4

DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
HASH=$(git rev-parse HEAD)
RUN_ID="$DATE-$HASH"

./docker-run-log-pipeline-event.sh  "$CONFIGURATION_MODULE" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$RUN_ID" "PipelineRunStart"

./docker-sync-rapid-pro-to-engagement-db.sh --incremental-cache-volume "cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                                    "$CONFIGURATION_MODULE"

./docker-sync-engagement-db-to-coda.sh --incremental-cache-volume "cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                        "$CONFIGURATION_MODULE"

./docker-sync-coda-to-engagement-db.sh --incremental-cache-volume "cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                        "$CONFIGURATION_MODULE"

./docker-run-engagement-db-to-analysis.sh --incremental-cache-volume "cache" "$USER" "$GOOGLE_CLOUD_CREDENTIALS_PATH" \
                        "$CONFIGURATION_MODULE" "$OUTPUT_DIR"

./docker-run-log-pipeline-event.sh  "$CONFIGURATION_MODULE" "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$RUN_ID" "PipelineRunEnd"
