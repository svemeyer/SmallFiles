#!/bin/bash

getFlagDirs() {
   local requestDir="$1"
   find "${requestsDir}" -mindepth 3 -maxdepth 3 -type d
}

getOsmTemplate() {
   local groupDir="$1"
   expr "${groupDir}" : ".*/\([^/]*\)/[^/]*/[^/]*$"
}

getStorageGroup() {
   local groupDir="$1"
   expr "${groupDir}" : ".*/\([^/]*\)/[^/]*$"
}

getDirHash() {
   local groupDir="$1"
   expr "${groupDir}" : ".*/\([^/]*\)$"
}

getGroupSubDir() {
   local groupDir="$1"
   local osmTemplate=$(getOsmTemplate "$groupDir")
   local storageGroup=$(getStorageGroup "$groupDir")
   local dirHash=$(getDirHash "$groupDir")
   groupSubDir="${osmTemplate}/${storageGroup}/${dirHash}"
}
