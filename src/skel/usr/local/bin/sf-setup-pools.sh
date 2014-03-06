#!/bin/bash

for pool in sf11 sf12 sf13 sf14 sf15 sf21 sf22 sf23 sf24 sf25; do 
  ssh admin@localhost -p 22224 <<EOF
cd $pool
st set max active 5
rh set max active 5
hsm set dcache -command=/usr/share/dcache/lib/hsm-internal.sh
hsm set dcache -mongoUrl=ceph-mds1.desy.de/smallfiles
flush set interval 60
flush set retry delay 180
rm set max active 1
mover set max active -queue=regular 100
mover set max active -queue=p2p 10
pp set max active 10
pp set pnfs timeout 300
EOF
done

