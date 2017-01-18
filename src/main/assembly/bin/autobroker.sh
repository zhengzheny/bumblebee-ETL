startbroker()
{
    broker=`/usr/bin/ps -ef | grep kafka | grep server$2.properties |wc -l`
    if [ $broker -ne 1 ]
    then
       JMX_PORT=$1 /usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server$2.properties
       echo $curr" start kafka broker $2..."
    fi
}

count=`/usr/local/jdk1.8.0_45/bin/jps -l | grep kafka | wc -l`
curr=`date +"%Y-%m-%d %H:%M:%S"`
if [ $count -lt 4 ]
then
    startbroker 19091 1
    startbroker 19092 2 
    startbroker 19093 3 
    startbroker 19094 4
else
    echo $curr" no start kafka broker..."
fi
