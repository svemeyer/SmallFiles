#!/bin/bash

if [ $# -ne 2 ]
then
    echo "Usage: pushFiles.sh <file count> <directory>"
    exit 1
fi

echo "creating $1 small files in $2"
i=1
for i in $(seq $1)
do
    echo $i > $(mktemp --dry-run --tmpdir="${2}" sfXXXXXXXXXX)
done

echo "done."
