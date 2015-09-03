#!/bin/sh

#
# This script is used by Jenkins to run tests for the replik8db project.
# - Tests are run prior to code review (all tests must pass).
# - Tests may be run before package build and after deployment.
# 
# Note: Run this script from the main directory...
#       > ./tests/jenkins_tests.sh
#

modules='
./lib/BoonJson.py
./lib/Properties.py
./lib/WorkerThreads.py --short
# These tests require you host to be an HDFS and HBase client.
./lib/HDFS.py
./lib/HBase.py
'

IFS="
"
for m in ${modules}; do
    # echo $m
    if [ ! -z ${m} ]; then
	if [ ! ${m:0:1} == "#" ]; then
	    echo "Running [$m]..."
	    eval "$m"
	    if [ $? != 0 ]; then
		exit $?
	    fi
	fi
    fi
done

