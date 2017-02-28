#jps -l | grep ETL  | awk -F' ' '{print $1}' | xargs kill -9
jps -l | grep org.apache.flume.node.Application  | awk -F' ' '{print $1}' | xargs kill 
