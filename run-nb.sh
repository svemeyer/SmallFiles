#!/bin/sh
cd src/main/ipy
ipython notebook --pylab='inline' --matplotlib='inline' --ip=`hostname -i`
