#!/bin/bash

set -e

IMAGE_NAME="$(<configurations/docker_image_name.txt)"

# Check that the correct number of arguments were provided.
if [[ $# -ne 5 ]]; then
    echo "Usage: ./docker-run-log-pipeline-event.sh
    [--profile-cpu <cpu-profile-output-path>] <configuration-file> <code-schemes-dir>
     <google-cloud-credentials-file-path> <run-id> <event-key>"
    echo "Updates pipeline event/status to a firebase table to aid in monitoring"
    exit 1
fi

# Assign the program arguments to bash variables.
CONFIGURATION_FILE=$1
CODE_SCHEMES_DIR=$2
GOOGLE_CLOUD_CREDENTIALS_PATH=$3
RUN_ID=$4
EVENT_KEY=$5

CMD="pipenv run python -u log_pipeline_event.py configuration /credentials/google-cloud-credentials.json \
       ${RUN_ID} ${EVENT_KEY}"

container="$(docker container create -w /app "$IMAGE_NAME" /bin/bash -c "$CMD")"

echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_PATH -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$container:/credentials/google-cloud-credentials.json"

echo "Copying $CODE_SCHEMES_DIR -> $container_short_id:/app/code_schemes"
docker cp "$CODE_SCHEMES_DIR" "$container:/app/code_schemes"

echo "Copying $CONFIGURATION_FILE -> $container_short_id:/app/configuration.py"
docker cp "$CONFIGURATION_FILE" "$container:/app/configuration.py"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Tear down the container when it has run successfully
docker container rm "$container" >/dev/null
