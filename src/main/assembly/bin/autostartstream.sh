count=`/usr/local/jdk1.8.0_45/bin/jps -l | grep ETL | wc -l`
curr=`date +"%Y-%m-%d %H:%M:%S"`
if [ $count -lt 16 ]
then
    ((c=16-$count))
    for((i=0;i<$c;i++))
    do
       nohup /usr/local/kafkastream/bin/etl-kafkastream.sh configFile=conf/GZDPI.xml &
       echo $curr" start stream agent..."
    done
else
    echo $curr" no start stream agent..."
fi
