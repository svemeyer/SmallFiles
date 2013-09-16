#!/bin/sh
#
logfile=/tmp/stacker.log
#
problem() {
   echo "$$ `date +"%k:%M:%S"` [$1] $2" >&2
   echo "$$ `date +"%k:%M:%S"` [$1] $2" >>${logfile}
   exit $1
}
say() {
   echo "$$ `date +"%k:%M:%S"` $1" >>${logfile}
   return 1
}
say "Started Request $*"
#
[ $# -lt 1 ] && problem 2 "Insufficient arguements for $0"
#
command=$1
#
if [ \( "$command" = "mount" \) -o \( "$command" = "dismount" \) ] ; then

   [ $# -lt 1 ] && problem 2 "Insufficient arguements for $0 ${command}"
   drive=$2
   driveLocation=$3
   cartridge=$4
   cartridgeLocation=$5

   if [ "${command}" = "mount" ] ; then

#
#      do the mount here
#
      sleep 4
#
#    if it failes run :
#
#     problem 33 "Mount failed due to ..."
#


   elif [ "${command}" = "dismount" ] ; then

      sleep 4

   else
      problem 4 "Panic ??"
   fi

elif [ "${command}" = "getProperty" ] ; then

   property=$2

   if [ "${property}" = "numberOfArms" ] ; then
      echo "2"
      exit 0 
   else
      problem 4 "No such property : ${property}"
   fi

else
   problem 3 "Command not supported : ${command}"
fi
#
say "Done"
exit 0
