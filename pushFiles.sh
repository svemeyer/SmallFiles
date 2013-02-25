#!/bin/bash

if [ $# -ne 2 ]
then
    echo "Usage: pushFiles.sh <file count> <directory>"
    exit 1
fi

echo creating $1 small files in $2
i=1
while [ $i -le $1 ]
do
    echo $i > "${2}/smallfile.$i"
    i=$(($i+1))
done

echo "done."
