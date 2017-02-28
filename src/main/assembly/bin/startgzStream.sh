if [ $# -lt 2 ]
then
  echo "usage:bin/startgzStream.sh configFile streamAgentCount"
  exit -1
fi

configFile=$1
streamAgentCount=$2
configPath=./conf/instance

if [ ! -d "$configPath" ]
then
  mkdir -p $configPath
else
  rm -f $configPath/*
fi

count=`jps -l | grep com.gsta.bigdata.etl.ETLRunner | wc -l`
if [ $count -lt $streamAgentCount ]
then
    ((c=$streamAgentCount-$count))
    curr=`date +"%Y-%m-%d %H:%M:%S"`
    for((i=1;i<=$c;i++))
    do
        config=$configPath/GZDPI$i.xml
        path="/home/kafkastream/state/config$i/"
        sed "s:STATE_DIR:${path}:g" $configFile > $config
        
        nohup /usr/local/kafkastream/bin/etl-kafkastream.sh configFile=$config $i &
        echo $curr" start $i  stream agent..."
    done
fi
