#!/bin/sh

testDir="tests"
sName="test"
testfile="/root/testfile"
fcount=120

echo "Cleaning test directories"
rm -rf "/pnfs/4/${testDir}"
rm -rf "/pnfs/4/hsm/requests/${sName}"
rm -rf "/pnfs/4/hsm/archives/${sName}"

echo "Cleaning crontab"
sed -i 's%^\*  \*  \*  \*  \* root /usr/share/dcache/lib/pack-files.sh \"/data\" \"/pnfs/4\" \"hsm\" 400000000 2>&1$%%' /etc/crontab

echo "Creating test directories"

mkdir -p "/pnfs/4/${testDir}/user/exp1"
echo "dcache" > "/pnfs/4/${testDir}/user/exp1/.(tag)(hsmInstance)"
echo "StoreName ${sName}" > "/pnfs/4/${testDir}/user/exp1/.(tag)(OSMTemplate)"
echo "smallfiles" > "/pnfs/4/${testDir}/user/exp1/.(tag)(sGroup)"
mkdir -p "/pnfs/4/${testDir}/user/exp1/sub1"
mkdir -p "/pnfs/4/${testDir}/user/exp1/sub2"

mkdir -p "/pnfs/4/${testDir}/user/exp2"
echo "dcache" > "/pnfs/4/${testDir}/user/exp2/.(tag)(hsmInstance)"
echo "StoreName ${sName}" > "/pnfs/4/${testDir}/user/exp2/.(tag)(OSMTemplate)"
echo "smallfiles" > "/pnfs/4/${testDir}/user/exp2/.(tag)(sGroup)"
mkdir -p "/pnfs/4/${testDir}/user/exp2/sub1"
mkdir -p "/pnfs/4/${testDir}/user/exp2/sub2"

mkdir -p "/pnfs/4/${testDir}/user/exp3"
echo "dcache" > "/pnfs/4/${testDir}/user/exp3/.(tag)(hsmInstance)"
echo "StoreName ${sName}" > "/pnfs/4/${testDir}/user/exp3/.(tag)(OSMTemplate)"
echo "smallfiles" > "/pnfs/4/${testDir}/user/exp3/.(tag)(sGroup)"
mkdir -p "/pnfs/4/${testDir}/user/exp3/sub1"
mkdir -p "/pnfs/4/${testDir}/user/exp3/sub2"

echo "Creating files in test directories"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp1" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp1/sub1" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp1/sub2" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp2" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp2/sub1" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp2/sub2" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp3" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp3/sub1" "${testfile}"
pushFiles.sh ${fcount} "/pnfs/4/${testDir}/user/exp3/sub2" "${testfile}"

echo "Creating crontab entry"
echo '*  *  *  *  * root /usr/share/dcache/lib/pack-files.sh "/data" "/pnfs/4" "hsm" 400000000 2>&1' >> /etc/crontab

