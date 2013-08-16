#!/bin/sh

targetRoot="/pnfs/4"
testDir="tests"
sName="test"
testfile="40k"
fcount=2000
dirCount=3
subDirCount=2
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
echo "*  *  *  *  * root /usr/local/bin/pack-files.sh \"/data\" \"${targetRoot}\" \"hsm\" 8000000 2>&1" >> /etc/crontab

echo "Creating files in test directories"

for exp in $(seq 1 $dirCount); do
  pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp$exp" "${testfile}" "${fdelay}"
  for sub in $(seq 1 $subDirCount); do
    pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp$exp/sub$sub" "${testfile}" "${fdelay}"
  done
done
