#!/usr/bin/env bash

set -e

if [[ $# -ne 2 ]]; then
    echo "Usage: ./6_backup_data_root <data-root> <backup-location>"
    echo "Backs-up the data root directory to a compressed file in at the specified location"
    exit
fi

OUTPUT_DIR=$1
BACKUP_FILE=$2

mkdir -p "$(dirname "$BACKUP_FILE")"
find "$OUTPUT_DIR" -type f -name '.DS_Store' -delete
cd "$OUTPUT_DIR"
echo "tarring into -czvf $OUTPUT_DIR"
tar -czvf "$BACKUP_FILE" .
