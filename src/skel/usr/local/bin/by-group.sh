#!/bin/bash

getFlagDirs() {
   local requestDir="$1"
   find "${requestsDir}" -mindepth 2 -maxdepth 2 -type d
}

getOsmTemplate() {
   local groupDir="$1"
   expr "${groupDir}" : ".*/\([^/]*\)/[^/]*$"
}

getStorageGroup() {
   local groupDir="$1"
   expr "${groupDir}" : ".*/\([^/]*\)$"
}

getGroupSubDir() {
   local groupDir="$1"
   local osmTemplate=$(getOsmTemplate "$groupDir")
   local storageGroup=$(getStorageGroup "$groupDir")
   groupSubDir="${osmTemplate}/${storageGroup}"
}
