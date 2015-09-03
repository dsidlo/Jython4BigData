#!/bin/sh

#
# This script is used by Jenkins to run tests for the replik8db project.
# - Tests are run prior to code review (all tests must pass).
# - Tests may be run before package build and after deployment.
#

modules='
./lib/BoonJson.py
# Sandbox tests
./lib/HDFS.py
./lib/HBase.py
./lib/Properties.py
./lib/WorkerThreads.py --short

# ./lib/AcomKafka.py
./lib/DbTable.py
./lib/TableSchemas.py

./replik8db.py --test
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

