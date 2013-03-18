#!/bin/bash

if [ $# -lt 2 ]
then
    echo "Usage: pushFiles.sh <file count> <directory> [<template file>]"
    exit 1
fi

echo "creating $1 files in $2"
for i in $(seq $1)
do
    filename=$(mktemp --dry-run --tmpdir="${2}" sfXXXXXXXXXX)
    if [ -z ${3} ]
    then
        echo $i > "${filename}"
    else
        cp "${3}" "${filename}"
    fi
done

echo "done."
