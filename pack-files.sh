#!/bin/bash
#
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
# the files are packed in an archive (tar) file in $hsmDir. Afterwards the files 
# in $flagdir are filled with a special uri that contains the file's and the 
# archive's pnfsids. This uri is then picked up and stored with the file in chimera 
# by hsm-internal.sh. 
#
# Parameters: 
#
#   $dcachePrefix:
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
    echo "Usage: pack-files.sh <dcachePrefix> <mountPoint> <hsmBase> <minSize>" | tee -a $LOG >&2
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

getFileSizeByPnfsId() {
    local fileName=$(cat "${userFileDir}/.(nameof)(${1}))")
    local filePath="${userFileDir}/${fileName}"
    echo $(getFileSize ${filePath})
}

getUserFileDirectoryFromFlag() {
    local tmp=$(cat "${mountPoint}/.(pathof)(${1})" | sed "s%${dcachePrefix}%${mountPoint}%")
    echo $(dirname "${tmp}")
}

filterDeleted() {
    while read id
    do
        local fileName=$(cat "${userFileDir}/.(nameof)(${id}))")
        local filePath="${userFileDir}/${fileName}"
        [ -f ${filePath} ] && echo ${id}
    done
}

filterAnswered() {
    while read id
    do
        [ ! -f "${groupDir}/${id}.answer" ] && echo ${id}
    done
}
# reads pnfsids and collects as many as needed to create the archive
# $1 - number of bytes to collect
# sets the following two variables
collectFiles() {
    sumSize=0
    while read id
    do
        if  [ ${1} -ne 0 ]
        then
            [ ${sumSize} -ge ${1} ] && break
        fi
        sumSize=$((${sumSize}+$(getFileSizeByPnfsId "${id}")))
        echo "${id}"
    done
    # append total size. (hack #1)
    echo ${sumSize}
}
######################################################
#
#   traps
#
cleanupLock() {
    report "    removing lock ${lockDir}"
    rmdir "${lockDir}"
}
cleanupTmpDir() {
    report "    cleaning up tmp directory ${tmpDir}"
    rm -rf "${tmpDir}"
}
cleanupArchive() {
    report "    deleting archive ${tarFile}"
    rm -f "${tarFile}"
}
######################################################
#
#   main
#
# check usage
if [ $(whoami) != "root" ]
then
    report "pack-files.sh needs to run as root."
    exit 5
fi

if [ $# -ne 4 ]
then 
    usage
    exit 4
fi

# assign parameters
dcachePrefix="${1}"
mountPoint="${2}"
hsmBase="${3}"
targetSize="${4}"

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
    report "    processing flag files in ${groupDir}"

    lockDir="${groupDir}/.lock"
    if ! mkdir "${lockDir}"
    then
      report "    leaving locked directory ${groupDir}"
      continue
    fi
    trap "cleanupLock; exit 130" SIGINT SIGTERM

    firstFlag=$(ls -U "${groupDir}"|grep -e "${pnfsidRegex}"|head -n 1)
    # if directory is empty continue with next group directory
    if [ -z $firstFlag ]
    then
        cleanupLock
        report "    leaving empty directory ${groupDir}"
        continue
    fi

    # create path of the user file dir
    userFileDir=$(getUserFileDirectoryFromFlag "${firstFlag}")
    # remember tags of user files for later
    osmTemplate=$(cat "${userFileDir}/.(tag)(OSMTemplate)" | sed 's/StoreName \(.*\)/\1/')
    storageGroup=$(cat "${userFileDir}/.(tag)(sGroup)")
    hsmInstance=$(cat "${userFileDir}/.(tag)(hsmInstance)")
    uriTemplate="${hsmInstance}://${hsmInstance}/?store=${osmTemplate}&group=${storageGroup}"
    report "      using $uriTemplate for files in $userFileDir"

    IFS=$'\n'
    flagFiles=($(ls -U "${groupDir}"|grep -e "${pnfsidRegex}"|filterAnswered|filterDeleted|collectFiles "${targetSize}"))
    IFS=$' '

    # read sum of files which comes as the last element of $flagFiles (hack #1) and unset it afterwards
    fileCount=$((${#flagFiles[@]}-1))
    sumSize=${flagFiles[${fileCount}]}
    unset flagFiles[${fileCount}]}
    # if the combined size is not enough, continue with next group dir
    if [ ${sumSize} -lt ${targetSize} ]
    then
        report "      combined size ${sumSize} < ${targetSize}. No archive created."
        cleanupLock
        cleanupTmpDir
        report "    leaving ${groupDir}"
        continue
    fi

    # create temporary directory
    tmpDir=$(mktemp --directory)
    trap "cleanupLock; cleanupTmpDir; exit 130" SIGINT SIGTERM
    report "      created temporary directory ${tmpDir}"

    # initialise manifest file
    mkdir "${tmpDir}/META-INF"
    manifest="${tmpDir}/META-INF/MANIFEST.MF"
    echo "Date: $(date)" > "${manifest}"

    # loop over files and collect until their size exceeds $minSize
    for pnfsid in ${flagFiles[@]}; do
        chimeraPath=$(cat "${mountPoint}/.(pathof)(${pnfsid})")
        fileName=$(basename "${chimeraPath}")
        realFile="${userFileDir}/${fileName}"
        ln -s "${realFile}" "${tmpDir}/${pnfsid}"

        echo "${pnfsid}:${chimeraPath}" >> "${manifest}"
    done

    echo "Total ${sumSize} bytes in ${fileCount} files" >> "${manifest}"
    report "      archiving ${fileCount} files with a total of ${sumSize} bytes"

    # create directory for the archive and then pack all files by their pnfsid-link-name in an archive
    tarDir="${archivesDir}/${osmTemplate}/${storageGroup}"
    report "      creating directory ${tarDir}"
    mkdir -p "${tarDir}"
    echo "StoreName ${osmTemplate}" > "${tarDir}/.(tag)(OSMTemplate)"
    echo "${storageGroup}" > "${tarDir}/.(tag)(sGroup)"

    tarFile=$(mktemp --dry-run --suffix=".tar" --tmpdir="${tarDir}" DARC-XXXXX)
    trap "cleanupLock; cleanupTmpDir; cleanupArchive; exit 130" SIGINT SIGTERM

    report "      packing archive ${tarFile}"
    cd "${tmpDir}"
    tar chf "${tarFile}" *
    # if creating the tar failed, we stop right here
    tarError=$?
    if [ ${tarError} -ne 0 ] 
    then 
        cleanupLock
        cleanupTmpDir
        cleanupArchive
        problem ${tarError} "Error: Creation of archive ${tarFile} file failed. Exiting"
    fi
    trap "cleanupLock; cleanupTmpDir; exit 130" SIGINT SIGTERM

    # if we succeeded we take the pnfsid of the just generated tar and create answer files in the group dir
    tarPnfsid=$(cat "${tarDir}/.(id)($(basename ${tarFile}))")
    report "      success. Stored archive ${tarFile} with PnfsId ${tarPnfsid}."

    report "      storing URIs in ${groupDir}"
    IFS=$'\n'
    for pnfsid in $(ls -U "${tmpDir}"|grep -e "${pnfsidRegex}"); do
        uri="${uriTemplate}&bfid=${pnfsid}:${tarPnfsid}"
        echo "${uri}" > "${groupDir}/.(use)(5)(${pnfsid})"
        echo "${uri}" > "${groupDir}/${pnfsid}.answer"
    done
    IFS=$' '

    cleanupLock
    cleanupTmpDir
    report "    leaving ${groupDir}"
done
report "finished."
