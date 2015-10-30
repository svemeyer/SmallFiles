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
  echo "Usage: install [pack [--update]|pool]"
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
  cp "${SRC_BIN}/dcap.py" "${LOCAL_BIN}"
  if [ ${2} != "--update" ];
  then
    echo "Copying configuration file to ${LOCAL_ETC}"
    mkdir -p "${LOCAL_ETC}"
    cp "${SRC_ETC}/container.conf" "${LOCAL_ETC}"
    echo "Creating log directory ${LOCAL_LOG}"
    mkdir -p "${LOCAL_LOG}"
  fi

  echo "Make sure your MongoDB is accessible from the pools and the packing machines"
  echo "and has an index on ctime on the smallfiles database."
  echo ""
  echo "To get started edit the configuration in /etc/dcache/container.conf."
fi

if [ ${1} = "pool" ];
then
  echo "Copying hsm scripts to ${DCACHE_LIB}"
  cp "${SRC_LIB}/hsm-internal.sh" "${DCACHE_LIB}"
  cp "${SRC_LIB}/datasetPut.js" "${DCACHE_LIB}"

  echo "To setup a pool to use the hsm-internal.sh script, set the following properties (adjusted to your system) on your pool:"
  echo ""
  echo "hsm set dcache -mongoUrl=packer/smallfiles"
  echo "hsm set dcache -dcapLib=/usr/lib64/libdcap.so.1"
  echo "hsm set dcache -dcapDoor=dcap-door:22125"
  echo "hsm set dcache -command=/usr/share/dcache/lib/hsm-internal.sh"
  echo ""
  echo "On the directory that will hold the small files, set the sGroup and OSMTemplate tags to s.th. appropriate"
  echo "Then set the hsmInstance tag on that directory to 'dcache'"
fi

echo "finished."

