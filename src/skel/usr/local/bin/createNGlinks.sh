#!/bin/sh

COMMANDS="cchecksum cchmod cdummy cls cmkdir cln cparent creadlevel crm csetfacl ctouch cwritelevel cchgrp cchown cgetfacl clstag cnameof cpathof creadtag crmtag cstat cwritedata cwritetag sfput"

usage() {
  echo "Usage: createNGlinks.sh <bin path> [<ng path>]"
  echo "<bin path> is the path to the directory to create the symlinks to ng in"
  echo "<ng path> is the path to the ng binary"
  exit 1
}

if [ -z $1 ]; then
  usage
fi

BIN_PATH="$1"
NG_PATH="$2"

if [ -z "NG_PATH" ]; then
  NG_PATH="`which ng`"
fi

if [ -z $NG_PATH ]; then
  echo "Error: ng not found in path and not passed as argument."
  usage
fi

echo "creating symlinks for chimera nails to $NG_PATH in $BIN_PATH."
for cmd in $COMMANDS; do
  ln -s "${NG_PATH}" "$BIN_PATH/$cmd"
done

echo "done."

