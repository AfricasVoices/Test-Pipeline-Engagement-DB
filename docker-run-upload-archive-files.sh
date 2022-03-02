#!/bin/bash

set -e

PROJECT_NAME="$(<configurations/docker_image_project_name.txt)"
IMAGE_NAME=$PROJECT_NAME-upload-archive-files

# Check that the correct number of arguments were provided.
if [[ $# -ne 4 ]]; then
    echo "Usage: ./docker-run-upload-archive-files.sh
     <user> <google-cloud-credentials-file-path> <configuration-module> <archive-dir>"
    exit
fi
# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
CONFIGURATION_MODULE=$3
ARCHIVE_DIR=$4

# Build an image for this pipeline stage.
docker build -t "$IMAGE_NAME" .

# Create a container from the image that was just built.
CMD="pipenv run python -u upload_archive_files.py ${USER} /credentials/google-cloud-credentials.json \
    ${CONFIGURATION_MODULE} /archives"

# Create the container. Note that we use a bind mount here rather than a volume or docker cp so we can directly
# edit the archive files on the host system.
container="$(docker container create -w /app --mount type=bind,source="$ARCHIVE_DIR",target=/archives "$IMAGE_NAME" /bin/bash -c "$CMD")"

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
