#!/bin/bash
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
cleanupExit() {
  report "    Cleaning up for ${groupDir}"
  rmdir -f "${groupDir}/.lock"
  rm -rf "${tmpDir}"
}

cleanupInt() {
  report "    Interrupted. Cleaning up"
  rmdir -f "${groupDir}/.lock"
  rm -rf "${tmpDir}"
  rm -f "${tarFile}"
}

######################################################
#
#   main
#
dcachePrefix="${1}"
mountPoint="${2}"
archivesDir="${3}"
groupDir="${4}"
minSize="${5}"

report "processing flag files in ${groupDir}"
if mkdir "${groupDir}/.lock"
then
    trap cleanupExit EXIT
    cd "${groupDir}"
else
    report "leaving locket directory $groupDir"
    exit 0
fi

# collect all files in group directory sorted by their age, oldest first
IFS=$'\n'
flagFiles=($(ls -t -r -1))
IFS=$' '
flagFilesCount=${#flagFiles[@]}
# if directory is empty continue with next group directory
if [ $flagFilesCount -eq 0 ]
then
    report "leaving empty directory $groupDir"
    exit 0
fi

# create path of the user file dir
tmpUserFilePath=$(cat ".(pathof)(${flagFiles})" | sed "s%${dcachePrefix}%${mountPoint}%")
userFileDir=$(dirname ${tmpUserFilePath})
# remember tags of user files for later
osmTemplate=$(cat "${userFileDir}/.(tag)(OSMTemplate)" | sed 's/StoreName \(.*\)/\1/')
storageGroup=$(cat "${userFileDir}/.(tag)(sGroup)")
hsmType=$(cat "${userFileDir}/.(tag)(HSMType)")
hsmInstance=$(cat "${userFileDir}/.(tag)(hsmInstance)")
uriTemplate="$hsmType://$hsmInstance/?store=$osmTemplate&group=$storageGroup"
report "  using $uriTemplate for $flagFilesCount files in $(pwd)"

# loop over files and collect until their size exceeds $minSize
sumSize=0
fileToArchiveNumber=0
while [[ ${minSize} == 0 || ${sumSize} -le ${minSize} ]] && [[ ${fileToArchiveNumber} -lt ${flagFilesCount}  ]]; do
    dotFile=".(nameof)(${flagFiles[${fileToArchiveNumber}]})"
    realFile=${userFileDir}/$(cat "${dotFile}")
    sumSize=$(($sumSize + $(stat -c%s ${realFile})))
    fileToArchiveNumber=$(($fileToArchiveNumber+1))
done

# if the combined size is not enough, stop here
if [ ${sumSize} -lt ${minSize} ]
then
    report "combined size smaller than ${minSize}. No archive created."
    exit 0
fi

# create sub-list of pnfsids of the files to archive
idsOfFilesForArchive=(${flagFiles[@]:0:${fileToArchiveNumber}})
report "  packing ${#idsOfFilesForArchive[@]} files:"

# create temporary directory and create symlinks named after the file's
# pnfsid to the corresponding user files in it
tmpDir=$(mktemp --directory)
mkdir "${tmpDir}/META-INF"
report "    created temporary directory ${tmpDir}"
cd "${tmpDir}"

report "    creating symlinks and manifest for files from ${userFileDir} in ${tmpDir}"
manifest="Date: $(date)\n"
for pnfsid in ${idsOfFilesForArchive[@]}; do
    filepath=$(cat "${mountPoint}/.(pathof)(${pnfsid})")
    # skip if the user file for the pnfsid does not exist
    [ $? -ne 0 ] && continue
    # skip if an answer file already exists
    [ -f "${pnfsid}.answer" ] && continue

    realFile=${userFileDir}/$(basename ${filepath})
    ln -s "${realFile}" "${pnfsid}"
    manifest="${manifest}${pnfsid}:${filepath}\n"
done
echo -e $manifest >> "${tmpDir}/META-INF/MANIFEST.MF"
unset manifest

# create directory for the archive and then pack all files by their pnfsid-link-name in an archive
tarDir="${archivesDir}/${osmTemplate}/${storageGroup}"
report "    creating output directory ${tarDir}"
mkdir -p "${tarDir}"
echo "StoreName ${osmTemplate}" > "${tarDir}/.(tag)(OSMTemplate)"
echo "${storageGroup}" > "${tarDir}/.(tag)(sGroup)"

tarFile=$(mktemp --dry-run --suffix=".tar" --tmpdir="${tarDir}" sfa.XXXXX)
report "    packing archive ${tarFile}"
trap cleanupInt SIGINT SIGTERM
tar chf "${tarFile}" *
# if creating the tar failed, we have a problem and will stop right here
tarError=$?
if [ ${tarError} -ne 0 ]
then
    rm -f "${tarFile}"
    problem ${tarError} "Creation of archive ${tarFile} file failed. Cleaning up"
fi

# if we succeeded we take the pnfsid of the just generated tar and create answer files in the group dir
tarPnfsid=$(cat "${tarDir}/.(id)($(basename ${tarFile}))")
report "  success. Stored archive ${tarFile} with PnfsId ${tarPnfsid}"
cd "${groupDir}"

report "  assigning archive URIs to files"
for pnfsid in ${idsOfFilesForArchive[@]} ; do
    answerFile=${pnfsid}.answer
    uri="${uriTemplate}&bfid=${pnfsid}:${tarPnfsid}"
    echo "${uri}" > ".(use)(5)(${pnfsid})"
    echo "${uri}" > "${answerFile}"
done

report "finished processing ${groupDir}"
exit 0