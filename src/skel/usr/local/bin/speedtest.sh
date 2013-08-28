#!/bin/sh

TIME="/usr/bin/time"

if [ $# -ne 3 ]
then 
    echo "Usage: speedtest.sh <command> <testdir> <outputfile>"
    echo "  Note: speedtest.sh depends on files named 1k 2k 4k 8k 1M 2M 4M 8M exiting in the current directory."
    echo "        it also depends on the script pushFiles.sh being in the path."
    echo ""
    echo "  Commands:"
    echo "    create: measures performance of creating new files"
    echo "    write:  measures performance of writing into file levels (dCache NFS4.1 only)"
    echo "    read:   measures performance of reading from file levels (dCache NFS4.1 only)"
    echo "    ls:     measures performance of 'ls -1 -U' on the directories"
    echo "    find:   measures performance of 'find' on the directories"
    echo "    stat:   measures performance of 'stat' on the files per directory"
    echo "    tar:    creates an archive from all files in the directory"
    exit 1
fi

CMD="$1"
BASEDIR="$2"
OUTFILE="$3"

SIZEDIRS="1k 2k 4k 8k 40k 1M 2M 4M 8M"
FILEDIRS="1000 2000 4000 10000 20000 100000"


echo "Invalidating fs cache"
echo 2 > /proc/sys/vm/drop_caches

echo '\begin{tabular}{|r|r||r|r|r|}' >> "${OUTFILE}"
echo '  \hline' >> "${OUTFILE}"
echo '  file size & file count & user & system & total \\' >> "${OUTFILE}"
echo '  \hline' >> "${OUTFILE}"
for sdir in ${SIZEDIRS};
do
    dir="${BASEDIR}/${sdir}"
    mkdir -p "${dir}"
    for fdir in ${FILEDIRS}; 
    do
        fulldir="${dir}/${fdir}"
        mkdir -p "${fulldir}"
        echo "testing on $fdir $sdir files in $fulldir."
        case $CMD in
          "create" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" pushFiles.sh ${fdir} "${fulldir}" ${sdir}
            ;;
          "write" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" sh -c 'for file in ${0}/* ; do filename=$(basename "$file") ; echo "speedtest" > "${0}/.(use)(5)($filename)" ; done' "$fulldir"
            ;;
          "read" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" sh -c 'for file in ${0}/* ; do filename=$(basename "$file") ; cat "${0}/.(use)(5)($filename)" ; done' "$fulldir"
            ;;
          "ls" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" ls -1 -U "${fulldir}"
            ;;
          "find" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" find "${fulldir}"
            ;;
          "stat" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" sh -c 'for file in ${0}/* ; do stat -c%s "${file}" ; done' "$fulldir"
            ;;
          "tar" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" tar cf "${fulldir}/all.tar" "${fulldir}/sf*"
            ;;
          "zip" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" zip "${fulldir}/all.zip" "${fulldir}/sf*"
            ;;
          "nop" )
            $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" /bin/true
            ;;
          * )
            echo "Unknown command $CMD."
            exit 2
            ;;
        esac
        echo '  \hline' >> "${OUTFILE}"
    done
done
echo '\end{tabular}' >> "${OUTFILE}"

