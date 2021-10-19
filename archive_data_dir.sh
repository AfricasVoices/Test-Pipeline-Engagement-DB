#!/usr/bin/env bash

set -e

if [[ $# -ne 2 ]]; then
    echo "Usage: ./archive_data_dir <data-dir> <archive-file>"
    echo "Backs-up the data root directory to a compressed file in at the specified location"
    exit
fi

DATA_DIR=$1
ARCHIVE_FILE=$2

mkdir -p "$(dirname "$ARCHIVE_FILE")"
find "$DATA_DIR" -type f -name '.DS_Store' -delete
cd "$DATA_DIR"
echo "tarring into -czvf $DATA_DIR"
tar -czvf "$ARCHIVE_FILE" .
