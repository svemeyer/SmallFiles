#!/bin/sh

TIME="/usr/bin/time"

if [ $# -ne 2 ]
then 
    echo "Usage: speedtest.sh <testdir> <outputfile>"
    echo "Note: speedtest.sh depends on files named 1k 2k 4k 8k 1M 2M 4M 8M exiting in the current directory."
    echo "      it also depends on the script pushFiles.sh being in the path."
    exit 1
fi

BASEDIR="$1"
OUTFILE="$2"

SIZEDIRS="1k 2k 4k 8k 40k 1M 2M 4M 8M"
FILEDIRS="1000 2000 4000 10000 20000 100000"

echo "starting tests" | tee -a "${OUTFILE}"
for sdir in ${SIZEDIRS};
do
    dir="${BASEDIR}/${sdir}"
    mkdir "${dir}"
    for fdir in ${FILEDIRS}; 
    do
        fulldir="${dir}/${fdir}"
        mkdir "${fulldir}"
        echo "creating $fdir $sdir files int $fulldir." | tee -a "${OUTFILE}"
        $TIME -o ${OUTFILE} -a pushFiles.sh ${fdir} "${fulldir}" ${sdir}
    done
done
echo "finished." | tee -a "${OUTFILE}"

