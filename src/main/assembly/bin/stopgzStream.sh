#jps -l | grep ETL  | awk -F' ' '{print $1}' | xargs kill -9
jps -l | grep com.gsta.bigdata.etl.ETLRunner  | awk -F' ' '{print $1}' | xargs kill 
