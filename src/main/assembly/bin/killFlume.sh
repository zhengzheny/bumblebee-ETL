if [ $# -lt 1 ]
then
  echo "usage:bin/killFlume.sh type [agentNo]"
  exit -1
fi

type=$1
agentNo=$2

if [ "$type" == "all" ]
then
  jps -l | grep org.apache.flume.node.Application  | awk -F' ' '{print $1}' | xargs kill
else
  cmd="ps -ef|grep flume |grep $type |grep -v grep "
  if [ "$agentNo" != "" ]
  then
    cmd="$cmd|grep $type$agentNo"
  fi
  cmd="$cmd |awk '{print \$2}'|xargs kill"
  echo $cmd
  eval $cmd
fi

