count=`jps -l | grep org.apache.flume.node.Application | wc -l`
totalCount=10
if [ $count -lt $totalCount ]
then
    ((c=$totalCount-$count))
    for((i=0;i<$c;i++))
    do
        nohup bin/etl-flume.sh ./conf/gzdpiKafka2HDFS.conf >>logs/nohup.log  &
        echo "start $i  flume agent..."
    done
fi
