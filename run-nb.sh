#!/bin/sh
cd src/main/ipy
ipython notebook --ip=`hostname -i` --port=8880
