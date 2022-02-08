#!/bin/bash

set -e

PROJECT_NAME="$(<configurations/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-sync-coda-to-engagement-db

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)
            DRY_RUN="--dry-run"
            shift;;
        --incremental-cache-volume)
            INCREMENTAL_ARG="--incremental-cache-path /cache"
            INCREMENTAL_CACHE_VOLUME_NAME="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 4 ]]; then
    echo "Usage: $0 
    [--dry-run] [--incremental-cache-volume <incremental-cache-volume>]
    <user> <google-cloud-credentials-file-path> <configuration-module> <data-dir>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
CONFIGURATION_MODULE=$3
DATA_DIR=$4

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u sync_coda_to_engagement_db.py ${DRY_RUN} ${INCREMENTAL_ARG} \
    ${USER} /credentials/google-cloud-credentials.json ${CONFIGURATION_MODULE}"

if [[ "$INCREMENTAL_ARG" ]]; then
    container="$(docker container create -w /app --mount source="$INCREMENTAL_CACHE_VOLUME_NAME",target=/cache "$IMAGE_NAME" /bin/bash -c "$CMD")"
else
    container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
fi

echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_PATH -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Copy cache data out of the container for backup
if [[ "$INCREMENTAL_ARG" ]]; then
    echo "Copying $container_short_id:/cache/. -> $DATA_DIR/Cache"
    mkdir -p "$DATA_DIR/Cache"
    docker cp "$container:/cache/." "$DATA_DIR/Cache"
fi

# Tear down the container when it has run successfully
docker container rm "$container" >/dev/null
