jps -l | grep ETL  | awk -F' ' '{print $1}' | xargs kill -9
