#!/bin/sh

packFilesScript=`which pack-files.sh`
targetRoot=`mount|grep "minorversion=1,vers=4"|awk '{ print $3 }'`
remoteExport=`mount|grep "minorversion=1,vers=4"|awk '{ print $1 }'|sed 's/[^:].*://'`
testDir="tests"
sName="test"
testfile="1M"
filesize=1000000
fcount=1000
dirCount=3
subDirCount=2
archiveSize=$(( $filesize * 1000 ))
fdelay=""

sh clearDirs.sh

echo "Cleaning crontab"
sed -i "s%^\*  \*  \*  \*  \* root .*/pack-files.sh .*$%%" /etc/crontab

echo "Creating test directories"

for exp in $(seq 1 $dirCount); do
  mkdir -p "${targetRoot}/${testDir}/user/exp$exp"
  echo "dcache" > "${targetRoot}/${testDir}/user/exp$exp/.(tag)(hsmInstance)"
  echo "StoreName ${sName}" > "${targetRoot}/${testDir}/user/exp$exp/.(tag)(OSMTemplate)"
  echo "smallfiles" > "${targetRoot}/${testDir}/user/exp$exp/.(tag)(sGroup)"

  for sub in $(seq 1 $subDirCount); do
    mkdir -p "${targetRoot}/${testDir}/user/exp$exp/sub$sub"
    mkdir -p "${targetRoot}/${testDir}/user/exp$exp/sub$sub"
  done
done

echo "Creating crontab entry"
echo "*  *  *  *  * root ${packFilesScript} \"${remoteExport}\" \"${targetRoot}\" \"hsm\" ${archiveSize} 2>&1" >> /etc/crontab

echo "Creating files in test directories"

for exp in $(seq 1 $dirCount); do
  pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp$exp" "${testfile}" "${fdelay}"
  for sub in $(seq 1 $subDirCount); do
    pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp$exp/sub$sub" "${testfile}" "${fdelay}"
  done
done
