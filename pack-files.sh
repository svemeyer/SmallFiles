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
#  $dcachePrefix:
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
#  $minSize:
#             This is the minimum size of archives to be created.
#             E.g. "1073741824" (1gb)
#
#######################################################
#
#   prerequisites
#
LOG=/tmp/pack-files.log
IFS=$'\n'
mountPoint="/pnfs/4"
hsmBase="hsm"
dcachePrefix="/data"
minSize=0

######################################################
#
#   functions
# 
usage() {
    echo "Usage: pack-files.sh <dcachePrefix> <mountPoint> <hsmBase> <minSize>" | tee -a $LOG >&2
}
report() {
    echo "`date +"%D-%T"` ($$) $pnfsid $1" | tee -a $LOG >&2
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
echo "Looking for archivation requests in ${mountPoint}/${hsmBase}/requests"
cd "${mountPoint}/${hsmBase}/requests"

flagDirs=($(find . -mindepth 2 -maxdepth 2 -type d))
dirCount=${#flagDirs[@]}
echo "  found " $dirCount " request groups."

for group in ${flagDirs}
do
    groupDir="${mountPoint}/${hsmBase}/requests/${group:2}"
    
    echo "    processing flag files in ${groupDir}"
    cd "${groupDir}"
    flagFiles=($(ls -t -1))
    flagFilesCount=${#flagFiles[@]}
    
    [ $flagFilesCount -le 0 ] && continue

    dcacheFileDir=$(cat ".(pathof)(${flagFiles})" | sed "s%${dcachePrefix}%${mountPoint}%")
    realFileDir=$(dirname ${dcacheFileDir})
        
    osmTemplate=$(cat "${realFileDir}/.(tag)(OSMTemplate)" | sed 's/StoreName \(.*\)/\1/')
    storageGroup=$(cat "${realFileDir}/.(tag)(sGroup)")
    hsmType=$(cat "${realFileDir}/.(tag)(HSMType)")
    hsmInstance=$(cat "${realFileDir}/.(tag)(hsmInstance)")
    echo "    using $hsmType://$hsmInstance/?store=$osmTemplate&group=$storageGroup"
        
    sumSize=0
    fileToArchiveNumber=1
    while [ ${sumSize} -le ${minSize} ] && [ ${fileToArchiveNumber} -le ${flagFilesCount} ]; do
        realFile=${realFileDir}/$(cat ".(nameof)($flagFiles[$fileToArchiveNumber])")
        sumSize=$(($sumSize + $(stat -c%s ${realFile})))
        fileToArchiveNumber=$(($fileToArchiveNumber+1))
    done
    
    if [ ${sumSize} -ge ${minSize} ] ; then
        filesForArchive=${flagFiles[@]:0:$(($fileToArchiveNumber-1))}
        echo "    Packing ${#filesForArchive[@]} files:"
        echo ${filesForArchive[@]}
        echo

        tmpDir=$(mktemp --directory --dry-run --tmpdir="${hsmBase}/requests")
        echo "      creating temporary directory ${tmpDir}"
        mkdir -p "${tmpDir}"
        cd "${tmpDir}"

        echo "      creating symlinks from ${realFileDir} to ${tmpDir} for files"
        for flagFile in ${filesForArchive} ; do
            realFile=${realFileDir}/$(cat ".(nameof)(${flagFile})")
            echo "       linking $realFile to $flagFile"
            ln -s "${realFile}" "${flagFile}"
        done

        tarDir="${hsmBase}/archives/${osmTemplate}/${storageGroup}"
        echo "      creating output directory ${tarDir}"
        mkdir -p "${tarDir}"
        echo "StoreName ${osmTemplate}" > "${tarDir}/.(tag)(OSMTemplate)"
        echo "${storageGroup}" > "${tarDir}/.(tag)(sGroup)"
        tarFile=$(mktemp --dry-run --suffix=".tar" --tmpdir="${tarDir}" sfa.XXXX)

        echo "      packing archive ${tarFile}"
        tar cvhf "${tarFile}" *
        
        tarError=$?
        if [ ${tarError} -ne 0 ] ; then 
            rm -rf "${tmpDir}"
            problem ${tarError} "Creation of archive ${tarFile} file failed."
        fi
        
        tarPnfsid=$(cat "${tarDir}/.(id)($(basename ${tarFile}))")
        echo "      success. Stored archive with PnfsId ${tarPnfsid} into ${tarDir}."
            
        cd "${groupDir}"
        echo "      creating answer files "
        for pnfsid in ${filesForArchive} ; do
            answerFile=${pnfsid}.answer
            echo "$hsmType://$hsmInstance/?store=$osmTemplate&group=$storageGroup&bfid=${pnfsid}:${tarPnfsid}" > "${answerFile}"
        done
            
        echo "      deleting temporary directory"
        rm -rf "${tmpDir}"
    fi
done
