#!/bin/sh
#
# $Id: hsmcpV4.sh,v 1.1 2006-05-31 14:08:15 tigran Exp $
#
#set -x
#
#
#    dCache configuration
#
#   hsm set osm -command=<fullPathToThisScript>
#   hsm set osm -client=<pathToEurogateClient>
#
#########################################################
#
#   prerequisits
#
LOG=/tmp/hsmio.log
AWK=gawk
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

   pnfsID=$1
#
   fullname=`cat "${pnfsMountpoint}/.(nameof)($pnfsID)"`
   while :
     do
       pnfsID=`cat "${pnfsMountpoint}/.(parent)($pnfsID)" 2>/dev/null`
       if [ $? -ne 0 ] ; then return 1 ; fi
       if [ "$pnfsID" = "000000000000000000000000000000000000" ] ; then break ; fi
       fullname=`cat "${pnfsMountpoint}/.(nameof)($pnfsID)"`"/"$fullname
   done
   echo "/"$fullname
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
command=$1
pnfsid=$2
filename=$3
#
# make sure the storage info variables are available
# (Will be fetched with getStorageInfoKey)
#
report "Checking SI"
#
[ -z "${si}" ] && problem 1 "StorageInfo (-si=...) not available"
#
###############################################################
#
if [ $command = "get" ] ; then
    
   "${client}" get "${pnfsid}" "${filename}"
   if [ $? -ne 0 ] ; then
      report "Retrieving from Eurogate HSM failed : ${pnfsid} -> ${filename}"
      exit 4
   fi
   #
   report "Restore finished, done"
   #
   exit 0 
   #
#################################################################
#
#   and the put
#
elif [ $command = "put" ] ; then
   #
   #
   #
   filesize=`chimera-cli stat "${filename}" 2>/dev/null | awk '{ print $5 }'`
   #
   #  check for existence of file
   #  NOTE : if the filesize is zero, we are expected to return 31, so that
   #         dcache can react accordingly.
   #
   [ -z "${filesize}" ] && problem 31 "File not found : ${filename}"
   [ "${filesize}" = "0" ] && problem 31 "Filesize is zero (${filename})" 
   #
   #  now, finally copy the file to the HSM
   #  (we assume the bfid to be returned)
   #
   result=`"${client}" put "${filename}"` || exit $?
   #
   report "Result : ${result}"
   #
   echo "${result}"
   #
   exit 0
   #
elif [ $command = "next" ] ; then
   echo "'next' operation not supported by this HSM" 1>&2
   exit 10 
else 
   echo "Illegal command $command" 1>&2
   exit 4
fi
exit 0
