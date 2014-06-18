#!/bin/sh

SRC_BIN="src/skel/usr/local/bin"
SRC_ETC="src/skel/etc/dcache"
SRC_LIB="src/skel/usr/share/dcache/lib"
LOCAL_BIN="/usr/local/bin/"
LOCAL_ETC="/etc/dcache"
LOCAL_LOG="/var/log/dcache"
DCACHE_LIB="/usr/share/dcache/lib/"

if [ ! $# = 1 ];
then
  echo "This script needs root privileges"
  echo 
  echo "Usage: install <mode>"
  echo "Modes: "
  echo "  pack: Install packing scripts"
  echo "  pool: Install hsm scripts"
  echo
  exit 1
fi

if [ ${1} = "pack" ];
then
  echo "Copying packing scripts to ${LOCAL_BIN}"
  cp "${SRC_BIN}/pack-files.py" "${LOCAL_BIN}"
  cp "${SRC_BIN}/fillmetadata.py" "${LOCAL_BIN}"
  cp "${SRC_BIN}/writebfids.py" "${LOCAL_BIN}"
  echo "Copying configuration file to ${LOCAL_ETC}"
  mkdir -p "${LOCAL_ETC}"
  cp "${SRC_ETC}/container.conf" "${LOCAL_ETC}"
  echo "Creating log directory ${LOCAL_LOG}"
  mkdir -p "${LOCAL_LOG}"
fi

if [ ${1} = "pool" ];
then
  echo "Copying hsm scripts to ${DCACHE_LIB}"
  cp "${SRC_LIB}/hsm-internal.sh" "${DCACHE_LIB}"
  cp "${SRC_LIB}/datasetPut.js" "${DCACHE_LIB}"
fi

echo "finished."

