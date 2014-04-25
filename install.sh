#!/bin/sh

SRC_BIN="src/skel/usr/local/bin"
SRC_LIB="src/skel/usr/share/dcache/lib"
LOCAL_BIN="/usr/local/bin/"
DCACHE_LIB="/usr/share/dcache/lib/"

if [ ! $# = 1 ];
then
  echo "Usage: install <mode>"
  echo "Modes: "
  echo "  pack: Install packing scripts"
  echo "  pool: Install hsm scripts"
  echo
  exit 1
fi

if [ ${1} -eq "pack" ];
then
  echo "Copying packing scripts to ${LOCAL_BIN}"
  cp "${SRC_BIN}/pack-files.sh" "${LOCAL_BIN}"
  cp "${SRC_BIN}/fillmetadata.py" "${LOCAL_BIN}"
  cp "${SRC_BIN}/writebfids.py" "${LOCAL_BIN}"
fi

if [ ${1} -eq "pool" ];
then
  echo "Copying hsm scripts to ${DCACHE_LIB}"
  cp "${SRC_LIB}/hsm-internal.sh" "${DCACHE_LIB}"
  cp "${SRC_LIB}/datasetPut.js" "${DCACHE_LIB}"
fi

echo "finished."

