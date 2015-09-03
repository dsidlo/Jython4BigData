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
# The tests below require your host to be a client to HDFS and HBase.
./lib/HDFS.py
./lib/HBase.py
# The following tests requires an ElasticSearch cluster or host
# Please update ./conf/ElasticSearchTest.properties accordingly.
./lib/ElasticSearch.py
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

