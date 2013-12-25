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
   echo "${requestsBase}/${ystore}/${ygroup}"
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
}
