#!/usr/bin/env bash

set -e

if [[ $# -ne 2 ]]; then
    echo "Usage: ./archive_data_dir <data-dir> <backup-file>"
    echo "Backs-up the data root directory to a compressed file in at the specified location"
    exit
fi

DATA_DIR=$1
BACKUP_FILE=$2

mkdir -p "$(dirname "$BACKUP_FILE")"
find "$DATA_DIR" -type f -name '.DS_Store' -delete
cd "$DATA_DIR"
echo "tarring into -czvf $DATA_DIR"
tar -czvf "$BACKUP_FILE" .
