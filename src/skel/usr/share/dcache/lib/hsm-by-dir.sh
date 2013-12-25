getFileDirHash() {
   CHIMERA_PARAMS="$1"
   ybfid="$2"
   dirname `cpathof ${CHIMERA_PARAMS} "${ybfid}"` | md5sum | $AWK '{ print $1 }'
}

getRequestBase() {
   CHIMERA_PARAMS="$1"
   ydataRoot="$2"
   yhsmBase="$3"
   ystore="$4"
   ygroup="$5"
   ybfid="$6"
   echo "${ydataRoot}/${yhsmBase}/requests"
}

getRequestPath() {
   CHIMERA_PARAMS="$1"
   ydataRoot="$2"
   yhsmBase="$3"
   ystore="$4"
   ygroup="$5"
   ybfid="$6"
   requestBase=$(getRequestBase "${CHIMERA_PARAMS}" "${ydataRoot}" "${yhsmBase}" "${ystore}" "${ygroup}" "${ybfid}")
   fileDirHash=$(getFileDirHash "${CHIMERA_PARAMS}" "${ybfid}")
   echo "${requestsBase}/${ystore}/${ygroup}/${fileDirHash}"
}

mkRequestDir() {
   CHIMERA_PARAMS="$1"
   ydataRoot="$2"
   yhsmBase="$3"
   ystore="$4"
   ygroup="$5"
   ybfid="$6"
   requestBase=$(getRequestBase "${CHIMERA_PARAMS}" "${ydataRoot}" "${yhsmBase}" "${ystore}" "${ygroup}" "${ybfid}")
   cmkdir ${CHIMERA_PARAMS} "${requestsBase}/${ystore}" 2>/dev/null
   cmkdir ${CHIMERA_PARAMS} "${requestsBase}/${ystore}/${ygroup}" 2>/dev/null
   cmkdir ${CHIMERA_PARAMS} "${requestsBase}/${ystore}/${ygroup}/${fileDirHash}" 2>/dev/null
}
