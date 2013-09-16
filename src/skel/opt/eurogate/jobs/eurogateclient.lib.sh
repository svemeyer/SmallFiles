#
#
procHelp() {
   echo ""
   echo "    eurogateclient commands [arguments]"
   echo "      commands : "
   echo "                   put <filename>"
   echo "                   get <bfid> <filename>"
   echo "                   remove <bfid>"
   echo "                   show <bfid>"
   echo ""
   return 3
}
checkForJava() {
   which java 2>&1 1>/dev/null
   if [ $? -ne 0 ] ; then
     echo "Java VM not found; please set PATH correctly" >&2
     exit 5
   fi
}
prepareEurogate() {
   #
   checkForJava
   #
   CP=${thisDir}/../classes/eurogate.jar:${thisDir}/../classes/cells.jar
   if [ -z "$EG_HOSTNAME" ] ; then  EG_HOSTNAME=localhost ; fi
   if [ -z "$EG_PORT" ] ; then  EG_PORT=28000 ; fi
   if [ ! -z "$EG_REPLY" ] 
    then
      EG_REPLY_STRING="-reply=$EG_REPLY"
   else
      EG_REPLY_STRING=""
   fi
}
doThePut() {
   if [ $# -lt 1 ]
     then
       procHelp
       exit 3
   fi
   filename=$1
   if [ ! -f ${filename} ] ; then
     echo "Error: File ${filename} doesn't exist" >&2
     exit 3
   fi
   # 
   prepareEurogate
   #
   java -cp ${CP} eurogate.gate.EuroSyncClient "$EG_REPLY_STRING" -host=${EG_HOSTNAME} -port=${EG_PORT} write ${filename} all
   return $?
}
doTheGet() {
   if [ $# -lt 2 ]
     then
       procHelp
   fi
   filename=$2
   bfid=$1
   if [ -f ${filename} ] ; then
     echo "Error: File ${filename} already exists" >&2
     exit 3
   fi
   # 
   prepareEurogate
   #
   java -cp ${CP} eurogate.gate.EuroSyncClient "$EG_REPLY_STRING" -host=${EG_HOSTNAME} -port=${EG_PORT} read ${bfid} ${filename} 
   rc=$?
   if [ $rc -eq 33 ] ; then rm -rf ${filename} ; fi
   return $rc
}
doTheShow() {
   #
   if [ $# -ne 1 ] ; then procHelp ; exit 4 ;  fi 
   #
   bfid=$1
   # 
   prepareEurogate
   #
   java -cp ${CP} eurogate.gate.EuroSyncClient "$EG_REPLY_STRING" -host=${EG_HOSTNAME} -port=${EG_PORT}  get-bf ${bfid}
   rc=$?
#   if [ $rc -eq 34 ] 
#     then
#        echo "Error 34 : bfid not found : ${bfid}"
#   fi
   return $rc
}
doTheRemove() {
   #
   if [ $# -ne 1 ] ; then procHelp ; exit 4 ;  fi 
   #
   bfid=$1
   # 
   prepareEurogate
   #
   java -cp ${CP} eurogate.gate.EuroSyncClient "$EG_REPLY_STRING" -host=${EG_HOSTNAME} -port=${EG_PORT}  remove ${bfid}
   rc=$?
   if [ $rc -eq 34 ] 
     then
        echo "Error 34 : bfid not found : ${bfid}"
   fi
   return $rc
}
doTheList() {
   #
   if [ $# -ne 1 ] ; then procHelp ; exit 4 ;  fi 
   #
   volume=$1
   # 
   prepareEurogate
   #
   java -cp ${CP} eurogate.gate.EuroSyncClient "$EG_REPLY_STRING" -host=${EG_HOSTNAME} -port=${EG_PORT}  list-volume ${volume}
   rc=$?
#   if [ $rc -eq 34 ] 
#     then
#        echo "Error 34 : bfid not found : ${bfid}"
#   fi
   return $rc
}
doTheTest() {
   #
   # 
   prepareEurogate
   #
   java -cp ${CP} eurogate.gate.EuroSyncClient $*
   rc=$?
   return $rc
}
procSwitch() {
   case "$1" in
      *put)
           shift 1 
           doThePut $* ;;
      *get)      
           shift 1
           doTheGet $*  ;;
      *remove)      
           shift 1
           doTheRemove $*  ;;
      *show)      
           shift 1
           doTheShow $*  ;;
      *list)      
           shift 1
           doTheList $*  ;;
      *test)      
           shift 1
           doTheTest $*  ;;
      *) procHelp $*
      ;;
   esac
}

