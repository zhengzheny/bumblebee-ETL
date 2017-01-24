#jps -l | grep ETL  | awk -F' ' '{print $1}' | xargs kill -9
jps -l | grep ETL  | awk -F' ' '{print $1}' | xargs kill 
