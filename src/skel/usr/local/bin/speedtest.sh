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
    exit 1
fi

CMD="$1"
BASEDIR="$2"
OUTFILE="$3"

SIZEDIRS="1k 2k 4k 8k 40k 1M 2M 4M 8M"
FILEDIRS="1000 2000 4000 10000 20000 100000"

case $CMD in
  "create" )
    COMMAND='pushFiles.sh ${fdir} "${fulldir}" ${sdir}'
    ;;
  "write" )
    COMMAND='for file in "${fulldir}"/*; do filename=$(basename "$file"); echo "speedtest" > "${fullpath}/.(use)(5)($filename)"; done'
    ;;
  "read" )
    COMMAND='for file in "${fulldir}"/*; do filename=$(basename "$file"); cat "${fullpath}/.(use)(5)($filename)"; done'
    ;;
  "ls" )
    COMMAND='ls -1 -U "${fulldir}"'
    ;;
  "find" )
    COMMAND='find "${fulldir}" -name "*"'
    ;;
  "stat" )
    COMMAND='for file in "${fulldir}"/*; do stat -c%s "{file}"; done'
    ;;
  "nop" )
    COMMAND="/bin/true"
    ;;
  * )
    echo "Unknown command $CMD."
    exit 2
    ;;
esac

echo '\begin{tabular}{|r|r||r|r|}' >> "${OUTFILE}"
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
        $TIME --output="${OUTFILE}" --append --format="  $sdir & $fdir & %U & %S & %e \\\\\\\\" $COMMAND
        echo '  \hline' >> "${OUTFILE}"
    done
done
echo '\end{tabular}' >> "${OUTFILE}"

