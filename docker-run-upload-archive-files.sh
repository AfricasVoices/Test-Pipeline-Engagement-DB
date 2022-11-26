#!/bin/bash

set -e

IMAGE_NAME="$(<configurations/docker_image_name.txt)"

# Check that the correct number of arguments were provided.
if [[ $# -ne 5 ]]; then
    echo "Usage: ./docker-run-upload-archive-files.sh
     <user> <google-cloud-credentials-file-path> <configuration-module> <code-schemes-dir> <archive-dir>"
    exit 1
fi
# Assign the program arguments to bash variables.
USER=$1
GOOGLE_CLOUD_CREDENTIALS_PATH=$2
CONFIGURATION_FILE=$3
CODE_SCHEMES_DIR=$4
ARCHIVE_DIR=$5

CMD="pipenv run python -u upload_archive_files.py ${USER} /credentials/google-cloud-credentials.json \
    configuration /archives"

# Create the container. Note that we use a bind mount here rather than a volume or docker cp so we can directly
# edit the archive files on the host system.
container="$(docker container create -w /app --mount type=bind,source="$ARCHIVE_DIR",target=/archives "$IMAGE_NAME" /bin/bash -c "$CMD")"

echo "Created container $container"
container_short_id=${container:0:7}

# Copy input data into the container
echo "Copying $GOOGLE_CLOUD_CREDENTIALS_PATH -> $container_short_id:/credentials/google-cloud-credentials.json"
docker cp "$GOOGLE_CLOUD_CREDENTIALS_PATH" "$container:/credentials/google-cloud-credentials.json"

echo "Copying $CONFIGURATION_FILE -> $container_short_id:/app/configuration.py"
docker cp "$CONFIGURATION_FILE" "$container:/app/configuration.py"

echo "Copying $CODE_SCHEMES_DIR -> $container_short_id:/app/code_schemes"
docker cp "$CODE_SCHEMES_DIR" "$container:/app/code_schemes"

# Run the container
echo "Starting container $container_short_id"
docker start -a -i "$container"

# Tear down the container when it has run successfully
docker container rm "$container" >/dev/null
