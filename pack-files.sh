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
#   $minSize:
#             This is the minimum size of archives to be created.
#             E.g. "1073741824" (1gb)
#
#######################################################
#
#   prerequisites
#
LOG=/tmp/pack-files.log

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

######################################################
#
#   traps
#
cleanupLock() {
    rmdir "${lockDir}"
    report "cleaning up lock ${lockDir}"
}
cleanupTmpDir() {
    rm -rf "${tmpDir}"
    report "cleaning up tmp directory ${tmpDir}"
}
cleanupArchive() {
    rm -f "${tarFile}"
    report "deleting archive ${tarFile}"
}
######################################################
#
#   main
#
# check usage
if [ $# -ne 4 ]
then 
    usage
    exit 4
fi
# assign parameters
dcachePrefix="${1}"
mountPoint="${2}"
hsmBase="${3}"
minSize="${4}"
# construct absolute requests and archives dirs 
requestsDir="${mountPoint}/${hsmBase}/requests"
archivesDir="${mountPoint}/${hsmBase}/archives"
# checking for existing flag directories for archivation requests
report "Looking for archivation requests in ${requestsDir}"
cd "${requestsDir}"

IFS=$'\n'
flagDirs=($(find . -mindepth 2 -maxdepth 2 -type d))
IFS=$' '
dirCount=${#flagDirs[@]}
report "  found $dirCount request groups."

# iterate over all found OSMTemplate/sGroup directory combinations 
for group in ${flagDirs[@]}
do
    # construct absolute dir name. Especially the first two chars (i.e., "./") of the
    # relative group dir have to be omitted.
    groupDir="${requestsDir}/${group:2}"

    # /usr/share/dcache/lib/pack-dir.sh "${dcachePrefix}" "${mountPoint}" "${archivesDir}" "${groupDir}" ${minSize}

    report "    processing flag files in ${groupDir}"
    cd "${groupDir}"
    lockDir="${groupDir}/.lock"
    if ! mkdir "${lockDir}"
    then
      report "      skipping locked directory $groupDir"
      continue
    fi
    trap "cleanupLock; exit 130" SIGINT SIGTERM

    # collect all files in group directory sorted by their age, oldest first
    IFS=$'\n'
    flagFiles=($(ls -U|grep -e '^[A-Z0-9]\{36\}$'))
    IFS=$' '
    flagFilesCount=${#flagFiles[@]}
    # if directory is empty continue with next group directory
    if [ $flagFilesCount -eq 0 ]
    then
        report "      skipping empty directory $groupDir"
        cleanupLock
        continue
    fi

    # create path of the user file dir
    tmpUserFilePath=$(cat ".(pathof)(${flagFiles})" | sed "s%${dcachePrefix}%${mountPoint}%")
    userFileDir=$(dirname ${tmpUserFilePath})
    # remember tags of user files for later
    osmTemplate=$(cat "${userFileDir}/.(tag)(OSMTemplate)" | sed 's/StoreName \(.*\)/\1/')
    storageGroup=$(cat "${userFileDir}/.(tag)(sGroup)")
    hsmInstance=$(cat "${userFileDir}/.(tag)(hsmInstance)")
    uriTemplate="$hsmInstance://$hsmInstance/?store=$osmTemplate&group=$storageGroup"
    report "    using $uriTemplate for $flagFilesCount files in $(pwd)"

    # create temporary directory
    tmpDir=$(mktemp --directory)
    trap "cleanupLock; cleanupTmpDir; exit 130" SIGINT SIGTERM
    mkdir "${tmpDir}/META-INF"
    manifest="${tmpDir}/META-INF/MANIFEST.MF"
    echo "Date: $(date)" > "${manifest}"
    report "      created temporary directory ${tmpDir}"

    # loop over files and collect until their size exceeds $minSize
    sumSize=0
    for pnfsid in ${flagFiles[@]}; do
        # skip if an answer file already exists
        [ -f "${pnfsid}.answer" ] && continue
        dotFile=".(pathof)(${pnfsid})"
        chimeraPath=$(cat "${mountPoint}/${dotFile}")
        # skip if the user file for the pnfsid does not exist
        [ $? -ne 0 ] && continue
        realFile=${userFileDir}/$(basename "${chimeraPath}")
        sumSize=$(($sumSize + $(stat -c%s ${realFile})))
        ln -s "${realFile}" "${tmpDir}/${pnfsid}"

        echo "${manifest}${pnfsid}:${chimeraPath}" >> "${manifest}"
        [ ${sumSize} -ge $minSize ] && break
    done

    # if the combined size is not enough, continue with next group dir
    if [ ${sumSize} -lt ${minSize} ]
    then 
        report "      combined size smaller than ${minSize}. No archive created." 
        cleanupLock
        cleanupTmpDir
        continue
    fi

    cd "${tmpDir}"

    # create directory for the archive and then pack all files by their pnfsid-link-name in an archive
    tarDir="${archivesDir}/${osmTemplate}/${storageGroup}"
    report "      creating directory ${tarDir}"
    mkdir -p "${tarDir}"
    echo "StoreName ${osmTemplate}" > "${tarDir}/.(tag)(OSMTemplate)"
    echo "${storageGroup}" > "${tarDir}/.(tag)(sGroup)"


    tarFile=$(mktemp --dry-run --suffix=".tar" --tmpdir="${tarDir}" DARC-XXXXX)
    trap "cleanupLock; cleanupTmpDir; cleanupTar; exit 130" SIGINT SIGTERM

    report "      packing archive ${tarFile}"
    tar chf "${tarFile}" *
    # if creating the tar failed, we stop right here
    tarError=$?
    if [ ${tarError} -ne 0 ] 
    then 
        cleanupLock
        cleanupTmpDir
        cleanupArchive
        problem ${tarError} "creation of archive ${tarFile} file failed. Exiting"
    fi
    trap "cleanupLock; cleanupTmpDir; exit 130" SIGINT SIGTERM

    # if we succeeded we take the pnfsid of the just generated tar and create answer files in the group dir
    tarPnfsid=$(cat "${tarDir}/.(id)($(basename ${tarFile}))")
    report "      success. Stored archive ${tarFile} with PnfsId ${tarPnfsid}."

    cd "${groupDir}"

    report "      storing URIs"
    IFS=$'\n'
    flagFiles=($(ls -U "${tmpDir}"|grep -e '^[A-Z0-9]\{36\}$'))
    IFS=$' '
    for pnfsid in ${flagFiles[@]} ; do
        answerFile=${pnfsid}.answer
        uri="${uriTemplate}&bfid=${pnfsid}:${tarPnfsid}"
        echo "${uri}" > ".(use)(5)(${pnfsid})"
        echo "${uri}" > "${answerFile}"
    done

    cleanupLock
    cleanupTmpDir
done
report "finished."
