#!/bin/bash
#
#set -x
# $Id: pack-files.sh 2013-02-11 karsten
#
# This script should be run manually or as a cron job. It is part of a set of 
# scripts that will collect, pack, and restore files in the context of dCache 
# (www.dcache.org) to improve the performance when handling many small files. 
#
# This scipt uses several special directories that are descibed in more detail 
# below. In the directory "flagDir" it expects empty files, named like the pnfsids
# of the files to be packed (this is done by the script hsm-internal.sh). 
# This script (pack-files.sh) will take those empty files' names, and create a 
# temporary directory containing a collection of symbolic links to the corresponding
# (small) files. If this collection exceeds some configured size (default 1gb) 
# the files are packed in an archive file in $hsmDir. Afterwards the files
# in $flagdir are filled with a special uri that contains the file's and the 
# archive's pnfsids. This uri is then picked up and stored with the file in chimera 
# by hsm-internal.sh. 
#
# Parameters: 
#
#   $chimeraDataPrefix:
#             This is the nfs exported directory as configured in /etc/exports.
#             E.g., "/data"
#
#   $mountPoint:
#             This is the mount point where dCache is mounted with nfs 4.1
#             E.g., "/pnfs/4"
#
#   $hsmBase:
#             This is base directory that will be used by the scripts to perform
#             their task relative to $mountPoint respectively $dcachePrefix. Below this
#             directory there have to be two sub directories: "archives" and "requests".
#             "archives" has to be tagged with hsmInstance "osm" and "CUSTODIAL"/"NEARLINE"
#             and "requests" has to be configured to be "REPLICA"/"ONLINE".
#             (i.e., 
#               # chimera-cli writetag "${dcachePrefix}/${hsmBase}/requests" AccessLatency ONLINE
#               # chimera-cli writetag "${dcachePrefix}/${hsmBase}/requests" RetentionPolicy REPLICA
#               # chimera-cli writetag "${dcachePrefix}/${hsmBase}/archives" AccessLatency NEARLINE
#               # chimera-cli writetag "${dcachePrefix}/${hsmBase}/archives" RetentionPolicy CUSTODIAL
#               OR
#               # echo "ONLINE" > "${mountPoint}/${hsmBase}/requests/.(tag)(AccessLatency)"
#               # echo "REPLICA" > "${mountPoint}/${hsmBase}/requests/.(tag)(RetentionPolicy)"
#               # echo "NEARLINE" > "${mountPoint}/${hsmBase}/archives/.(tag)(AccessLatency)"
#               # echo "CUSTODIAL" > "${mountPoint}/${hsmBase}/archives/.(tag)(RetentionPolicy)"
#             )
#             E.g., "hsm"
#
#   $targetSize:
#             This is the minimum size of archives to be created.
#             E.g. "1073741824" (1gb)
#
#   $packRemainingInterval:
#             This is the interval in minutes after which all remaining files are packed into an archive
#             regardless of its size.
#
#######################################################
#
#   prerequisites
#
LOG=/tmp/pack-files.log
averageFileCount=20
pnfsidRegex="^[ABCDEF0123456789]\{36\}$"

######################################################
#
#   functions
# 
usage() {
   echo "Usage: pack-files.sh <chimeraDataPrefix> <mountPoint> <hsmBase> <minSize> [<packRemainingInterval>]" | tee -a $LOG >&2
}
report() {
   echo "`date +"%D-%T"` ($$) $1" | tee -a $LOG >&2
}
problem() {
   echo "($$) $2 ($1)" | tee -a $LOG >&2
   exit $1
}
errorReport() {
   echo "($$) $1" | tee -a ${LOG} >&2
   return 0
}

getFileSize() {
   echo $(stat -c%s "${1}")
}

getChimeraUserFileFromFlag() {
   cat "${groupDir}/.(use)(4)(${1})"
}

getUserFileFromFlag() {
   chimeraPath=$(getChimeraUserFileFromFlag ${1})
   echo ${chimeraPath} | sed "s%${chimeraDataPrefix}%${mountPoint}%"
}

getFileSizeByPnfsId() {
   local fileName=$(getUserFileFromFlag "${1}")
   echo $(getFileSize "${fileName}")
}

filterDeleted() {
   while read id
   do
      [ -f $(getUserFileFromFlag ${id}) ] && echo ${id} || rm ${id}
   done
}

filterAnswered() {
   while read id
   do
      [ -f "${groupDir}/.(use)(5)(${id})" ] || echo ${id}
   done
}

# reads pnfsids and collects as many as needed to create the archive
# $1 - number of bytes to collect
# sets the following two variables
collectFiles() {
   local sumSize=0
   while read id
   do
      if  [ ${1} -ne 0 ]
      then
         [ ${sumSize} -ge ${1} ] && break
      fi
      # check if user file was deleted, if so, delete flag and skip to next
      userFile=$(getUserFileFromFlag ${id})
      [ -z ${userFile} ] && continue
      if [ ! -f ${userFile} ]
      then
         rm ${id}
         printf '-' >&2
         continue
      fi
      fileSize=$(getFileSize "${userFile}")
      [ $? -ne 0 ] && continue
      [ -z ${fileSize} ] && continue
      sumSize=$(( ${sumSize}+${fileSize} ))
      echo "${id}"
      printf '+' >&2
   done
   printf "\n" >&2
   # append total size. (hack #1)
   echo ${sumSize}
}
######################################################
#
#   traps
#
cleanupLock() {
   report "    removing lock ${lockDir}"
   rm -f "${lockDir}/$$"
   rmdir "${lockDir}"
}
cleanupTmpDir() {
   report "    cleaning up tmp directory ${tmpDir}"
   rm -rf "${tmpDir}"
}
cleanupArchive() {
   report "    deleting archive ${archiveFile}"
   rm -f "${archiveFile}"
}
######################################################
#
#   main
#
# check usage
if [ $(whoami) != "root" ]
then
   report "pack-files.sh must run as root."
   exit 5
fi

if [ $# -ne 5 ]
then 
   usage
   exit 4
fi

# assign parameters
chimeraDataPrefix="${1}"
mountPoint="${2}"
hsmBase="${3}"
targetSize="${4}"
packRemainingInterval="${5}"

# construct absolute requests and archives dirs
requestsDir="${mountPoint}/${hsmBase}/requests"
archivesDir="${mountPoint}/${hsmBase}/archives"
# checking for existing flag directories for archivation requests
report "Looking for archivation requests in ${requestsDir}"

IFS=$'\n'
flagDirs=($(find "${requestsDir}" -mindepth 2 -maxdepth 2 -type d))
IFS=$' '
dirCount=${#flagDirs[@]}
report "  found $dirCount request groups."

# iterate over all found OSMTemplate/sGroup directory combinations 
for groupDir in ${flagDirs[@]}
do
   cd "${groupDir}"

   lockDir="${groupDir}/.lock"
   trap "cleanupLock; exit 130" SIGINT SIGTERM
   if ! mkdir "${lockDir}"
   then
      report "    leaving locked directory ${groupDir}"
      continue
   fi

   # create pid file in lock directory
   touch "${lockDir}/$$"

   report "    processing flag files in ${groupDir}"

   # remember tags of user files for later
   osmTemplate=$(expr "${groupDir}" : ".*/\([^/]*\)/[^/]*$")
   storageGroup=$(expr "${groupDir}" : ".*/\([^/]*\)$")
   hsmInstance="dcache"
   uriTemplate="${hsmInstance}://${hsmInstance}/?store=${osmTemplate}&group=${storageGroup}"
   groupSubDir="${osmTemplate}/${storageGroup}"
   report "      using $uriTemplate for files with in group $groupSubDir"

   IFS=$'\n'
   flagFiles=($(find "${groupDir}" -name "0000*" -printf "%f\n"|grep -e "${pnfsidRegex}"|filterAnswered|collectFiles "${targetSize}"))
   collectResult=$?
   IFS=$' '

   if [ ${collectResult} -ne 0 ]
   then
      report "      detected file deletion during collection. No archive created."
      cleanupLock
      report "    leaving ${groupDir}"
      continue
   fi

   # read sum of files which comes as the last element of $flagFiles (hack #1) and unset it afterwards
   fileCount=$((${#flagFiles[@]}-1))
   sumSize=${flagFiles[${fileCount}]}
   unset flagFiles[${fileCount}]

   # # DEBUG
   # report "      processing the following files:"
   # for flag in ${flagFiles[@]} ; do
   #     report "$flag"
   # done

   # if there were no valid files at all
   if [ ${fileCount} -le 0 ]
   then
      report "      no unanswered flags in ${groupDir}. No archive created."
      cleanupLock
      report "    leaving ${groupDir}"
      continue
   fi

   # if the combined size is not enough
   if (( ${sumSize} < ${targetSize} ))
   then
      if [ ! -z ${packRemainingInterval} ]
      then
         recentFile=$(find ${groupDir} -type f -cmin +${packRemainingInterval}|filterAnswered|head -n 1)
         if [ ! -f ${recentFile} ]
         then
            report "      combined size ${sumSize} < ${targetSize} and last change more recent than ${packRemainingInterval} minutes. No archive created."
            cleanupLock
            report "    leaving ${groupDir}"
            continue
         else
            report "      Last changed file in ${groupDir} older than ${packRemainingInterval} minutes. Packing remaining files."
         fi
      else # continue with next group dir
         report "      combined size ${sumSize} < ${targetSize}. No archive created."
         cleanupLock
         report "    leaving ${groupDir}"
         continue
      fi
   fi

   # create temporary directory
   tmpDir=$(mktemp --directory)
   trap "cleanupLock; cleanupTmpDir; exit 130" SIGINT SIGTERM
   report "      created temporary directory ${tmpDir}"

   # initialise manifest file
   mkdir "${tmpDir}/META-INF"
   manifest="${tmpDir}/META-INF/MANIFEST.MF"
   echo "Date: $(date)" > "${manifest}"

   for pnfsid in ${flagFiles[@]}; do
      filePath=$(getUserFileFromFlag "${pnfsid}")
      fileName=$(basename "${filePath}")
      ln -s "${filePath}" "${tmpDir}/${pnfsid}"

      chimeraFilePath=$(echo "$filePath"|sed "s%$mountPoint%$chimeraDataPrefix%")
      echo "${pnfsid}:${chimeraFilePath}" >> "${manifest}"
   done

   echo "Total ${sumSize} bytes in ${fileCount} files" >> "${manifest}"
   report "      archiving ${fileCount} files with a total of ${sumSize} bytes"

   # create directory for the archive and then pack all files by their pnfsid-link-name in an archive
   archivesGroupDir="${archivesDir}/${groupSubDir}"
   report "      creating directory ${archivesGroupDir}"
   mkdir -p "${archivesGroupDir}"
   [ -f "${archivesGroupDir}/.(tag)(OSMTemplate)" ] || echo "StoreName ${osmTemplate}" > "${archivesGroupDir}/.(tag)(OSMTemplate)"
   [ -f "${archivesGroupDir}/.(tag)(sGroup)" ] || echo "${storageGroup}" > "${archivesGroupDir}/.(tag)(sGroup)"

   # create archive
   archiveFile=$(mktemp --dry-run --suffix=".tar" --tmpdir="${archivesGroupDir}" DARC-XXXXX)
   trap "cleanupLock; cleanupTmpDir; cleanupArchive; exit 130" SIGINT SIGTERM

   report "      packing archive ${archiveFile}"
   cd "${tmpDir}"
   tar chf "${archiveFile}" *

   # if creating the archive failed, we stop right here
   archivingExitCode=$?

   # archivedFilesCount=$(tar tf "${archiveFile}"|grep -e "${pnfsidRegex}"|wc -l)
   # report "      checking archive: Expected ${fileCount}, actual ${archivedFilesCount} files"

   if [ ${archivingExitCode} -ne 0 ] # || (( ${archivedFilesCount} != ${fileCount} ))
   then 
      cleanupLock
      cleanupTmpDir
      cleanupArchive
      problem ${archivingExitCode} "Error: Creation of archive ${archiveFile} file failed. Exiting"
   fi
   trap "cleanupLock; cleanupTmpDir; exit 130" SIGINT SIGTERM

   # if we succeeded we take the pnfsid of the just generated archive and create the replies
   archivePnfsid=$(cat "${archivesGroupDir}/.(id)($(basename ${archiveFile}))")
   report "      success. Stored archive ${archiveFile} with PnfsId ${archivePnfsid}."

   report "      storing URIs in ${groupDir}"
   for pnfsid in ${flagFiles[@]}; do
      uri="${uriTemplate}&bfid=${pnfsid}:${archivePnfsid}"
      echo "${uri}" > "${groupDir}/.(use)(5)(${pnfsid})" 2>>${LOG}
      [ $? -eq 0 ] && printf "*" >&2 && continue || report "      URI [${uri}], could not be be written to level 5 of flag file ${pnfsid}."
   done
   printf "\n" >&2

   cleanupLock
   cleanupTmpDir
   report "    leaving ${groupDir}"
done
report "finished."
