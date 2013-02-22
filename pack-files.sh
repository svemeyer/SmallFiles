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
IFS=$'\n'

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

flagDirs=($(find . -mindepth 2 -maxdepth 2 -type d -path "./tmp.*" -prune -o -print))
dirCount=${#flagDirs[@]}
report "  found $dirCount request groups."

# iterate over all found OSMTemplate/sGroup directory combinations 
for group in ${flagDirs[@]}
do
    # construct absolute dir name. Especially the first two chars (i.e., "./") of the
    # relative group dir have to be omitted.
    groupDir="${requestsDir}/${group:2}"
    report "    processing flag files in ${groupDir}"
    cd "${groupDir}"

    # collect all files in group directory sorted by their age, oldest first
    flagFiles=($(ls -t -r -1))
    flagFilesCount=${#flagFiles[@]}
    # if directory is empty continue with next group directory
    [ $flagFilesCount -eq 0 ] && continue

    # create path of the user file dir
    tmpUserFilePath=$(cat ".(pathof)(${flagFiles})" | sed "s%${dcachePrefix}%${mountPoint}%")
    userFileDir=$(dirname ${tmpUserFilePath})
    # remember tags of user files for later
    osmTemplate=$(cat "${userFileDir}/.(tag)(OSMTemplate)" | sed 's/StoreName \(.*\)/\1/')
    storageGroup=$(cat "${userFileDir}/.(tag)(sGroup)")
    hsmType=$(cat "${userFileDir}/.(tag)(HSMType)")
    hsmInstance=$(cat "${userFileDir}/.(tag)(hsmInstance)")
    report "    using $hsmType://$hsmInstance/?store=$osmTemplate&group=$storageGroup for $flagFilesCount files in $(pwd)"

    # loop over files and collect until their size exceeds $minSize
    sumSize=0
    fileToArchiveNumber=0
    while [[ ${minSize} == 0 || ${sumSize} -le ${minSize} ]] && [[ ${fileToArchiveNumber} -lt ${flagFilesCount}  ]]; do
        dotFile=".(nameof)(${flagFiles[${fileToArchiveNumber}]})"
        realFile=${userFileDir}/$(cat "${dotFile}")
        sumSize=$(($sumSize + $(stat -c%s ${realFile})))
        fileToArchiveNumber=$(($fileToArchiveNumber+1))
    done

    # if the combined size is not enough, continue with next group dir
    [ ${sumSize} -lt ${minSize} ] && report "      combined size smaller than ${minSize}. No archive created." && continue

    IFS=$' '
    # create sub-list of pnfsids of the files to archive
    idsOfFilesForArchive=(${flagFiles[@]:0:${fileToArchiveNumber}})
    report "    Packing ${#idsOfFilesForArchive[@]} files:"

    # create temporary directory and create symlinks named after the file's
    # pnfsid to the corresponding user files in it
    tmpDir=$(mktemp --directory)
    report "      created temporary directory ${tmpDir}"

    trap "rm -rf \"${tmpDir}\"" EXIT
    cd "${tmpDir}"

    report "      creating symlinks from ${userFileDir} to ${tmpDir} for files"
    for pnfsid in ${idsOfFilesForArchive[@]}; do
        filename=$(cat "${mountPoint}/.(nameof)(${pnfsid})")
        # skip if the user file for the pnfsid does not exist
        [ $? -ne 0 ] && continue

        realFile=${userFileDir}/${filename}
        ln -s "${realFile}" "${pnfsid}"
    done

    # create the manifest file containing pnfsid to chimera path mappings
    report "      creating manifest file in ${tmpDir}/META-INF"
    mkdir "${tmpDir}/META-INF"
    manifest="Date: $(date)\n"
    for pnfsid in ${idsOfFilesForArchive[@]}; do
        filepath=$(cat "${mountPoint}/.(pathof)(${pnfsid})")
        # again, skip if user file does not exist
        [ $? -ne 0 ] && continue

        manifest="${manifest}${pnfsid}:${filepath}\n"
    done
    echo -e $manifest >> "${tmpDir}/META-INF/MANIFEST.MF"
    manifest=""
    
    # create directory for the archive and then pack all files by their pnfsid-link-name in an archive
    tarDir="${archivesDir}/${osmTemplate}/${storageGroup}"
    report "      creating output directory ${tarDir}"
    mkdir -p "${tarDir}"
    echo "StoreName ${osmTemplate}" > "${tarDir}/.(tag)(OSMTemplate)"
    echo "${storageGroup}" > "${tarDir}/.(tag)(sGroup)"

    tarFile=$(mktemp --dry-run --suffix=".tar" --tmpdir="${tarDir}" sfa.XXXXX)
    report "      packing archive ${tarFile}"
    tar chf "${tarFile}" *
    # if creating the tar failed, we have a problem and will stop right here
    tarError=$?
    [ ${tarError} -ne 0 ] && problem ${tarError} "Creation of archive ${tarFile} file failed."

    # if we succeeded we take the pnfsid of the just generated tar and create answer files in the group dir
    tarPnfsid=$(cat "${tarDir}/.(id)($(basename ${tarFile}))")
    report "      success. Stored archive ${tarFile} with PnfsId ${tarPnfsid}."
    cd "${groupDir}"

    report "      storing URIs"
    for pnfsid in ${idsOfFilesForArchive[@]} ; do
        answerFile=${pnfsid}.answer
        uri="$hsmType://$hsmInstance/?store=$osmTemplate&group=$storageGroup&bfid=${pnfsid}:${tarPnfsid}"
        echo "${uri}" > ".(use)(5)(${pnfsid})"
        echo "${uri}" > "${answerFile}"
    done
done
