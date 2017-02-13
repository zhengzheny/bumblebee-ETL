# dispatcher data source to multi target directory
# every flume agent deals with one target directory
#

if [ $# -lt 3 ]
then
  echo "usage:bin/fileDispatcher.sh srcPath type agentNumber"
  echo "srcPath:data source directory"
  echo "type:data type"
  echo "agentNumber:flume agent client number"
  exit -1
fi

#srcPath=/data/huaweidpi/S1udns/0
#type=dns
srcPath=$1
type=$2
flumeAgentNum=$3

workPath=/data/huaweidpi/work/$type/
tempSuffix="._COPYING_"
sourceFileSuffix="gz"

if [ ! -d "$workPath" ]
then
  mkdir -p $workPath
fi
k=0 
for((i=0;i<$flumeAgentNum;i++))
do
  path="$workPath/$type$i"
  if [ ! -d $path ]
  then
    mkdir -p $path
  fi
done

while true
do
  files=`ls -l $srcPath/*.$sourceFileSuffix 2>&1`
  if [[ ! $files =~ "No such file" ]]
  then
  	files=`ls -l $srcPath/*.$sourceFileSuffix| grep -v total| awk -F'/' '{print $NF}' |awk '{print $NF}'`
	  i=0
	  for file in $files
	  do
	    ((j=$k % $flumeAgentNum))
	    mv $srcPath/$file  $workPath/$type$j/$file$tempSuffix
	    mv $workPath/$type$j/$file$tempSuffix $workPath/$type$j/$file
	    ((i++))
	    ((k++))
	  done
	  
	  if [ $i -gt 0 ] 
	  then
	    t=`date "+%Y-%m-%d %H:%M:%S"`
	    echo "$t successfule move $i files from source dir to work dir,total $k files"
	  fi
	fi
  
  sleep 30
done