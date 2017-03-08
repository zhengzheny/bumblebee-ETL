rootdir=/data/adpi/tmp

curr=`date +"%Y-%m-%d %H:%M:%S"`
echo "======================================================="
echo "$curr begin delete null temp dpi data dir"

today=`date +%Y%m%d`
daydirs=`hadoop fs -ls $rootdir |grep -v items |grep -v $today | awk  '{print $NF}'`
for daydir in $daydirs
do
  dirsize=`hadoop fs -du -s $daydir | awk '{print $1}'`
  if [ $dirsize -eq 0 ]
  then
    echo "delete day dir $daydir ,size=$dirsize"
    hadoop fs -rm -r $daydir
  else
    echo "day dir $daydir has file size=$dirsize,delete hour dir"   
    hourdirs=`hadoop fs -ls $daydir |grep -v items | awk  '{print $NF}'`
    for hourdir in $hourdirs
    do
      dirsize=`hadoop fs -du -s $hourdir | awk '{print $1}'`
      if [ $dirsize -eq 0 ]
      then
        echo "delete hour dir $hourdir ,size=$dirsize"
        hadoop fs -rm -r $hourdir
      else
        echo "hour dir $hourdir has file,size=$dirsize"
      fi
    done
  fi 
done   

curr=`date +"%Y-%m-%d %H:%M:%S"`
echo "$curr end of work......"