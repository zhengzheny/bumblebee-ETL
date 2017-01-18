#!/bin/sh

if [ $# -lt 1 ]
then
  echo "usage:bin/etl-flume.sh ./conf/flume-ztekpi.conf"
  exit -1
fi

BASEDIR=`dirname "$0"`/..
cd $BASEDIR

bin/flume-ng agent --conf ./conf/ -n etlAgent -f $@ 
