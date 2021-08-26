#!/bin/bash

set -e

PROJECT_NAME="$(<configurations/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-rapid-pro-to-engagement-db

while [[ $# -gt 0 ]]; do
    case "$1" in
        --incremental-cache-path)
            INCREMENTAL_MODE=true
            INCREMENTAL_CACHE_PATH="$2"
            shift 2;;
        --local-archive)
            USE_ARCHIVE=true
            LOCAL_ARCHIVE="$2"
            shift 2;;
        --)
            shift
            break;;
        *)
            break;;
    esac
done

# Check that the correct number of arguments were provided.
if [[ $# -ne 3 ]]; then
    echo "Usage: ./docker-run-sync-rapid-pro-to-engagement-db.sh
    [--incremental-cache-path <incremental-cache-path>] [--local-archive <local_archive>]
    <user> <google-cloud-credentials-file-path> <configuration-module>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
INPUT_CONFIGURATION_MODULE=$3

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

if [[ "$INCREMENTAL_MODE" = true ]]; then
    if [[ -d "$INCREMENTAL_CACHE_PATH" ]]; then
        OPTIONAL_ARGS="--incremental-cache-path $INCREMENTAL_CACHE_PATH"
    else
        echo "Directory \"$INCREMENTAL_CACHE_PATH\" does not exist"; exit 2;
    fi
fi
if [[ "$USE_ARCHIVE" = true ]]; then
    if [[ -d "$LOCAL_ARCHIVE" ]]; then
        OPTIONAL_ARGS+=" --local-archive $LOCAL_ARCHIVE"
    else
        echo "Directory \"$LOCAL_ARCHIVE\" does not exist"; exit 2;
    fi
fi

# Create a container from the image that was just built.
if [[ "$OPTIONAL_ARGS" ]]; then
    CMD="pipenv run python -u sync_rapid_pro_to_engagement_db.py \"$OPTIONAL_ARGS\" \
    \"$USER\" /credentials/google-cloud-credentials.json \"$INPUT_CONFIGURATION_MODULE\"
    "
else
    CMD="pipenv run python -u sync_rapid_pro_to_engagement_db.py \"$USER\" \
    /credentials/google-cloud-credentials.json \"$INPUT_CONFIGURATION_MODULE\"
    "
fi

container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"
echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $INPUT_GOOGLE_CLOUD_CREDENTIALS -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$INPUT_GOOGLE_CLOUD_CREDENTIALS" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Tear down the container when done.
docker container rm "$container" >/dev/null
