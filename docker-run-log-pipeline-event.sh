#!/bin/bash

set -e

PROJECT_NAME="$(<configurations/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-log-pipeline-event

# Check that the correct number of arguments were provided.
if [[ $# -ne 4 ]]; then
    echo "Usage: ./docker-run-log-pipeline-event.sh
    [--profile-cpu <cpu-profile-output-path>] <configuration-module> <google-cloud-credentials-file-path> \
     <run-id> <event-key>"
    echo "Updates pipeline event/status to a firebase table to aid in monitoring"
    exit
fi
# Assign the program arguments to bash variables.
CONFIGURATION_MODULE=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
RUN_ID=$3
EVENT_KEY=$4

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u log_pipeline_event.py ${CONFIGURATION_MODULE}  /credentials/google-cloud-credentials.json \
       ${RUN_ID} ${EVENT_KEY}"

container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_PATH -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$container:/credentials/google-cloud-credentials.json"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Tear down the container when it has run successfully
docker container rm "$container" >/dev/null
