#!/bin/sh

packFilesScript=`which pack-files.sh`
nfs41MountPoint=`mount|grep "minorversion=1,vers=4"|awk '{ print $3 }'`
nfs41Export=`mount|grep "minorversion=1,vers=4"|awk '{ print $1 }'|sed 's/[^:].*://'`
testDir="tests"
sName="test"
dirCount=3
subDirCount=0
testFile="1k"
fileSize=1000
filesPerDir=30000
filesPerArchive=100
archiveSize=$(( $fileSize * $filesPerArchive ))
packRemainingInterval=60
fdelay=""

sh clearDirs.sh

echo "Cleaning crontab"
sed -i "s%^\*  \*  \*  \*  \* root .*/pack-files.sh .*$%%" /etc/crontab

echo "Creating test directories"

for exp in $(seq 1 $dirCount); do
  mkdir -p "${nfs41MountPoint}/${testDir}/user/exp$exp"
  echo "dcache" > "${nfs41MountPoint}/${testDir}/user/exp$exp/.(tag)(hsmInstance)"
  echo "StoreName ${sName}" > "${nfs41MountPoint}/${testDir}/user/exp$exp/.(tag)(OSMTemplate)"
  echo "smallfiles" > "${nfs41MountPoint}/${testDir}/user/exp$exp/.(tag)(sGroup)"

  for sub in $(seq 1 $subDirCount); do
    mkdir -p "${nfs41MountPoint}/${testDir}/user/exp$exp/sub$sub"
    mkdir -p "${nfs41MountPoint}/${testDir}/user/exp$exp/sub$sub"
  done
done

echo "Creating crontab entry"
echo "*  *  *  *  * root ${packFilesScript} \"${nfs41Export}\" \"${nfs41MountPoint}\" \"hsm\" ${archiveSize} ${packRemainingInterval} 2>&1" >> /etc/crontab

echo "Creating files in test directories"

for exp in $(seq 1 $dirCount); do
  pushFiles.sh ${filesPerDir} "${nfs41MountPoint}/${testDir}/user/exp$exp" "${testFile}" "${fdelay}"
  for sub in $(seq 1 $subDirCount); do
    pushFiles.sh ${filesPerDir} "${nfs41MountPoint}/${testDir}/user/exp$exp/sub$sub" "${testFile}" "${fdelay}"
  done
done
