#!/bin/bash

set -e

PROJECT_NAME="$(<configurations/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-sync-engagement-db-to-rapidpro

# Check that the correct number of arguments were provided.
if [[ $# -ne 3 ]]; then
    echo "Usage: $0
    <user> <google-cloud-credentials-file-path> <configuration-module>"
    exit
fi

# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
CONFIGURATION_MODULE=$3

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u sync_rapid_pro_to_engagement_db.py ${USER} \
    /credentials/google-cloud-credentials.json ${CONFIGURATION_MODULE}"


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