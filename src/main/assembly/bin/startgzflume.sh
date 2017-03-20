if [ $# -lt 1 ]
then
  echo "usage:bin/startgzStream.sh agentCount"
  exit -1
fi

totalCount=$1
configPath=./conf/instance/
if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

count=`jps -l | grep org.apache.flume.node.Application | wc -l`
if [ $count -lt $totalCount ]
then
    ((c=$totalCount-$count))
    for((i=1;i<=$c;i++))
    do
        logConf=$configPath/log4j$i.properties
        log="/home/flumelogs/flume$i.log"
        sed "s:GSTA_LOG_FILE_NAME:${log}:g" ./conf/log4j.properties > $logConf
        nohup bin/etl-flume.sh ./conf/gzdpiKafka2HDFS.conf -Dlog4j.configuration=file:$logConf  &
        echo "start $i  flume agent..."
    done
fi
