#!/bin/sh
#
# $Id: hsmcpV4.sh,v 1.1 2006-05-31 14:08:15 tigran Exp $
#
#set -x
#
#
#    dCache configuration
#
#   hsm set dcache -command=<fullPathToThisScript>
#   hsm set dcache -hsmBase=<fullPathToTheMigratingFileSystem>
#   hsm set dcache -pnfsMountpoint=<fullPathToPnfsMountpoint>
#   hsm set dcache -dataRoot=<rootDataDir> # e.g., /data
#
#########################################################
#
#   prerequisits
#
LOG=/tmp/hsmio.log
DEVTTY=$LOG
AWK=gawk
LIBPDCAP="/usr/lib64/libpdcap.so.1"
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
#  Resolved pnfsID into canonical file name.
#  Argument : pnfsID
#  Returns  : filename
#  expects  : pnfsMountpoint
#
resolvePnfsID() {
   chimera-cli pathof "${1}"
   return $?
}
#
#########################################################
#
#  Map the canonical file name into the local filename 
#  Argument : canonical file path (on the server)
#  Returns  : local filepath
#  expects  : pnfsMountpoint
#
#
mapCanonicalToLocal() {

  canonicalPath=$1

  r=`mount 2>/dev/null | 
     grep "${pnfsMountpoint}" |
     awk '{ split($1,a,":") ; print a[2] }'`

  localPath=`expr "$1" : "$r\(.*\)" 2>/dev/null`

  if [ $? -ne 0 ] ; then return $? ; fi

  localPath=`echo "${localPath}" |
             awk '{ if( substr($1,1,1) != "/" ){ 
                      printf"/%s\n",$1 
                    }else{
                      print $1
                    }

             }'`

  if [ $? -ne 0 ] ; then return 2 ; fi

  echo ${pnfsMountpoint}${localPath}
  return 0

}
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
   ydataRoot=${1}
   yhsmBase=${2}
   ystore=${3}
   ygroup=${4}
   ybfid=${5}
#
#  Generate PATH
#
    fileDirHash=$(dirname `chimera-cli pathof "${ybfid}"` | md5sum | awk '{ print $1 }')
    requestPath="${yhsmBase}/requests/${ystore}/${ygroup}/${fileDirHash}"
    requestFlag="${requestPath}/${ybfid}"
#
    report "Using request flag : ${requestFlag}"
#
    reply=$(chimera-cli readlevel "${ydataRoot}/hsm/requests/${ystore}/${ygroup}/${fileDirHash}/${ybfid}" 5)
    if [ ! -z "${reply}" ] ; then
#
       report "Request answer found : ${reply}"
       iserror=`expr "${reply}" : "ERROR \([0-9]*\).*"`
       if [ $? -eq 0 ] ; then
          report "Found error ${iserror}"
          rm -f "${requestFlag}"
          return ${iserror}
       else 
          rm -f "${requestFlag}"
          echo $reply
          return 0
       fi
#
    elif [ -f "${requestFlag}" ] ; then
#
       report "Still waiting" 
       problem 2 "Not yet ready"
#
    else
#
       report "Initializing request" 
       mkdir -p "${requestPath}" 2>/dev/null
       touch "${requestFlag}"
       problem 3 "Request Initialized (async)"
#
    fi
     
    problem 102  "We should never end up here" 
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
report "$*"
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
report "Splitting arguments"
args=""
while [ $# -gt 0 ] ; do
  if expr "$1" : "-.*" >/dev/null ; then
     a=`expr "$1" : "-\(.*\)" 2>/dev/null`
     key=`echo "$a" | awk -F= '{print $1}' 2>/dev/null`
     value=`echo "$a" | awk -F= '{for(i=2;i<NF;i++)x=x $i "=" ; x=x $NF ; print x }' 2>/dev/null`
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
   set `echo "$args" | awk '{ for(i=1;i<=NF;i++)print $i }'`
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
report "Checking pnfsMountpoint"
#
[ -z "${pnfsMountpoint}" ] && problem 3 "Variable 'pnfsMountpoint' not defined"
[ ! -d "${pnfsMountpoint}" ]  && problem 4 "pnfsMountpoint=${pnfsMountpoint} : not a directory"
#
report "Checking hsmBase"
#
[ -z "${hsmBase}" ] && problem 3 "Variable 'hsmBase' not defined"
#
report "Checking hsmBase directory"
#
[ ! -d "${hsmBase}" ]  && problem 4 "hsmBase=${hsmBase} : not a directory"
#
report "Checking dataRoot"
#
[ -z "${dataRoot}" ] && problem 3 "Variable 'dataRoot' not defined"
#
# make sure the storage info variables are available
# (Will be fetched with getStorageInfoKey)
#
report "Checking SI"
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
report "Simulating mount time"
#
if [ -z "${waitTime}" ] ; then waitTime=0 ; fi
#
if [ $waitTime != 0 ]
  then
     report "Waiting ${waitTime} seconds"
     sleep ${waitTime}
     report "Returning from waiting ${waitTime} seconds"
fi
#
###############################################################
#
if [ $command = "get" ] ; then
   #
   #  splitting URI into pieces
   #
   report "Splitting URI into pieces"
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
   # find archived path
   #
   archiveFile=`resolvePnfsID "${archiveId}"`
   if [ $? -ne 0 ] ; then problem 2 "Problem resolving $archiveId" ; fi
   #
   report "Archive file is (canonical) : ${archiveFile}"
   #
   originalId=`expr "${getBfid}" : "\(.*\):.*" 2>/dev/null`
   report "Data File Pnfs ID : ${originalId}"
   #
   localDir=`dirname "${filename}"`
   #
   report "Extracting file into $localDir"
   #
   cd "${localDir}"
   export LD_PRELOAD="${LIBPDCAP}"
   tar x --force-local --file="${archiveFile}" "${originalId}" 2>>$LOG
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
   filesize=`stat -c%s "${filename}" 2>/dev/null`
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
   result=`datasetPut "${dataRoot}" "${hsmBase}" "${store}" "${group}" "${pnfsid}"` || exit $?
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
