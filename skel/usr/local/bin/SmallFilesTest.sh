#!/bin/sh

targetRoot="/pnfs/4"
testDir="tests"
sName="test"
testfile="40k"
fcount=2000
fdelay=""

echo "Cleaning ${targetRoot}/${testDir} ..."
sh clearDirs.sh

echo "Cleaning crontab"
sed -i "s%^\*  \*  \*  \*  \* root .*/pack-files.sh .*$%%" /etc/crontab

echo "Creating test directories"

mkdir -p "${targetRoot}/${testDir}/user/exp1"
echo "dcache" > "${targetRoot}/${testDir}/user/exp1/.(tag)(hsmInstance)"
echo "StoreName ${sName}" > "${targetRoot}/${testDir}/user/exp1/.(tag)(OSMTemplate)"
echo "smallfiles" > "${targetRoot}/${testDir}/user/exp1/.(tag)(sGroup)"
mkdir -p "${targetRoot}/${testDir}/user/exp1/sub1"
mkdir -p "${targetRoot}/${testDir}/user/exp1/sub2"

mkdir -p "${targetRoot}/${testDir}/user/exp2"
echo "dcache" > "${targetRoot}/${testDir}/user/exp2/.(tag)(hsmInstance)"
echo "StoreName ${sName}" > "${targetRoot}/${testDir}/user/exp2/.(tag)(OSMTemplate)"
echo "smallfiles" > "${targetRoot}/${testDir}/user/exp2/.(tag)(sGroup)"
mkdir -p "${targetRoot}/${testDir}/user/exp2/sub1"
mkdir -p "${targetRoot}/${testDir}/user/exp2/sub2"

mkdir -p "${targetRoot}/${testDir}/user/exp3"
echo "dcache" > "${targetRoot}/${testDir}/user/exp3/.(tag)(hsmInstance)"
echo "StoreName ${sName}" > "${targetRoot}/${testDir}/user/exp3/.(tag)(OSMTemplate)"
echo "smallfiles" > "${targetRoot}/${testDir}/user/exp3/.(tag)(sGroup)"
mkdir -p "${targetRoot}/${testDir}/user/exp3/sub1"
mkdir -p "${targetRoot}/${testDir}/user/exp3/sub2"

echo "Creating crontab entry"
echo "*  *  *  *  * root /usr/local/bin/pack-files.sh \"/data\" \"${targetRoot}\" \"hsm\" 8000000 2>&1" >> /etc/crontab

echo "Creating files in test directories"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp1" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp2" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp3" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp1/sub1" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp1/sub2" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp2/sub1" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp2/sub2" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp3/sub1" "${testfile}" "${fdelay}"
pushFiles.sh ${fcount} "${targetRoot}/${testDir}/user/exp3/sub2" "${testfile}" "${fdelay}"

