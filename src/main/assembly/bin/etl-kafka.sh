#!/bin/sh

BASEDIR=`dirname "$0"`/..
cd $BASEDIR

#bin/flume-ng agent --conf ./conf/ -n etlAgent -f ./conf/flume-mro-zte.conf
#bin/flume-ng agent --conf ./conf/ -n etlAgent -f ./conf/flume-LTEUP.conf
bin/flume-ng agent --conf ./conf/ -n etlAgent -f ./conf/kafka2hdfsTest.conf