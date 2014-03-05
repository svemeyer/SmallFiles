#!/bin/sh
#
# $Id: hsmcpV4.sh,v 1.1 2006-05-31 14:08:15 tigran Exp $
#
#set -x
#
#
#    dCache configuration
#
#   hsm set dcache -command=<fullPathToThisScript> # e.g., /usr/share/dcache/lib/hsm-internal.sh
#   hsm set dcache -mongoUrl=<urlOfMongoDb> # e.g., server.example.org/database
#
#########################################################
#
#   prerequisits
#
LOG=/tmp/hsmio.log
DEVTTY=$LOG
AWK=gawk
LIBPDCAP="/usr/lib64/libpdcap.so.1"
DCAP_DOOR="ceph-mon1:22125"

#
#
#########################################################
#
#  some help functions
#
usage() {
   echo "Usage : put|get <pnfsId> <filePath> [-si=<storageInfo>] [-key[=value] ...]" | tee -a $LOG >&2
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
#
#
#########################################################
#
#  Resolve pnfsID into canonical file name.
#  Argument : pnfsID
#  Returns  : filename
#
resolvePnfsID() {
   mongo ${mongoUrl} --eval "print(db.files.findOne( { pnfsid: "${1}"} ).canonicalPath)"
   return $?
}
#
#########################################################
#
getStorageInfoKey() {

   echo $si | 
   $AWK -v key=$1 -F';' '{
   for(i=1;i<=NF;i++){
      split($i,a,"=") ;
      if(a[1]==key)print a[2]
      }
   }'
}
#
#     end of init
#
#########################################################
#
datasetPut() {
   ystore=${1}
   ygroup=${2}
   ybfid=${3}

   params="\"var id='${ybfid}'; var ystore='${ystore}'; var ygroup='${ygroup}';\""
   reply=$(mongo --quiet ${mongoUrl} --eval "${params}" /usr/share/dcache/lib/datasetPut.js)
   result=$?

   [ ${result} -ne 0 ] && problem 102 "Error running mongo script"

   [ -z ${reply} ] && problem 2 "Not yet ready"

   echo "${reply}"
}
#
###############################################################
###############################################################
###############################################################
#
#   The big main
#
###############################################################
#
#
#  say hallo to people
#
# report "$*"
#
###############################################################
#
#   have we been called correctly ?
#
if [ $# -lt 3 ] ; then 
   usage
   exit 4
fi
#
#
##################################################################################
#
# split the arguments into the options -<key>=<value> and the 
# positional arguments.
#
# report "Splitting arguments"
args=""
while [ $# -gt 0 ] ; do
   if expr "$1" : "-.*" >/dev/null ; then
      a=`expr "$1" : "-\(.*\)" 2>/dev/null`
      key=`echo "$a" | $AWK -F= '{print $1}' 2>/dev/null`
      value=`echo "$a" | $AWK -F= '{for(i=2;i<NF;i++)x=x $i "=" ; x=x $NF ; print x }' 2>/dev/null`
      if [ -z "$value" ] ; then a="${key}=" ; fi
      eval "${key}=\"${value}\""
      a="export ${key}"
      eval "$a"
   else
      args="${args} $1"
   fi
   shift 1
done
if [ ! -z "$args" ] ; then
   set `echo "$args" | $AWK '{ for(i=1;i<=NF;i++)print $i }'`
fi
#
##################################################################################
#
# assign the manditory arguments
#
command="${1}"
pnfsid="${2}"
filename="${3}"
#
###############################################################
#
# check for some basic variables
#
# report "Checking mongoUrl"
[ -z "${mongoUrl}" ] && problem 3 "Variable 'mongoUrl' not defined"
#
# make sure the storage info variables are available
# (Will be fetched with getStorageInfoKey)
#
# report "Checking SI"
#
[ -z "${si}" ] && problem 1 "StorageInfo (-si=...) not available" 
#
#########################################################
#
#   osm specific variables
#
store=`getStorageInfoKey store 2>/dev/null`
group=`getStorageInfoKey group 2>/dev/null`
bfid=`getStorageInfoKey bfid 2>/dev/null`
#
###############################################################
#
#      simulate mount time
#
# report "Simulating mount time"
#
if [ -z "${waitTime}" ] ; then waitTime=0 ; fi
#
if [ $waitTime != 0 ]
then
   # report "Waiting ${waitTime} seconds"
   sleep ${waitTime}
   # report "Returning from waiting ${waitTime} seconds"
fi
#
###############################################################
#
if [ $command = "get" ] ; then
   #
   #  splitting URI into pieces
   #
   # report "Splitting URI into pieces"
   #
   getStore=`expr "${uri}" : ".*/?store=\(.*\)&group.*"`
   getGroup=`expr "${uri}" : ".*group=\(.*\)&bfid.*"`
   getBfid=`expr "${uri}" : ".*bfid=\(.*\)"`
   #
   report "URI : ${uri}"
   report "Store=${getStore}; Group=${getGroup}; Bfid=${getBfid}"
   #
   [ \( -z "${getStore}" \) -o \( -z "${getGroup}" \) -o \( -z "${getBfid}" \) ] && \
      problem 22 "couldn't get sufficient info for 'copy' : store=>${getStore}< group=>${getGroup}< bfid=>${getBfid}<" 
   #
   # Is this an archive ?
   #
   archiveId=`expr "${getBfid}" : ".*:\(.*\)" 2>/dev/null`
   if [ $? -ne 0 ] ; then
      problem 1 "This is not an archive : ${getBfid}"
   fi
   #
   originalId=`expr "${getBfid}" : "\(.*\):.*" 2>/dev/null`
   report "Data File Pnfs ID : ${originalId}"
   #
   extractDir=`dirname "${filename}"`
   #
   report "Extracting file into $extractDir"
   #
   cd "${extractDir}"
   report "Preloading ${LIBPDCAP}"
   export LD_PRELOAD="${LIBPDCAP}"
   unzip "pnfs://${DCAP_DOOR}/${archiveId}" "${originalId}" 2>>$LOG
   rc=$?
   cd -
   if [ $rc -ne 0 ] ; then problem 4 "Tar couldn't replay the file" ; fi
   #
   report "Extraction finished, done"
   #
   exit 0 
   #
elif [ $command = "put" ] ; then
   #
   #   and the put
   #
   filesize=$(stat "${filename}" -c%s)
   #
   #  check for existence of file
   #  NOTE : if the filesize is zero, we are expected to return 31, so that
   #         dcache can react accordingly.
   #
   [ -z "${filesize}" ] && problem 31 "File not found : ${filename}"
   [ "${filesize}" = "0" ] && problem 31 "Filesize is zero (${filename})" 
   #
   #
   #  now, finally copy the file to the HSM
   #  (we assume the bfid to be returned)
   #
   result=`datasetPut "${store}" "${group}" "${pnfsid}"` || exit $?
   #
   # osm://osm/?store=sql&group=chimera&bfid=3434.0.994.1188400818542
   #
   report "Result : ${result}"
   #
   echo "${result}"
   #
   exit 0
elif [ $command = "next" ] ; then
   echo "'next' operation not supported by this HSM" 1>&2
   exit 10 
else 
   echo "Illegal command $command" 1>&2
   exit 4
fi
exit 0
