#!/bin/sh

BASEDIR=`dirname "$0"`/..
cd $BASEDIR

bin/flume-ng agent --conf ./conf/ -n etlAgent -f ./conf/flume-mro-zte.conf