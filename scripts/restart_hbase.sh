function startWait(){
  curl -s -u $user:$pass -H 'X-Requested-By: ambari' -X PUT -d \
    '{"RequestInfo": {"context" :"Start '"$1"' via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' \
    http://$host/api/v1/clusters/$cluster/services/$1
  wait $1 "STARTED"
}
 
function stopWait(){
  curl -s -u $user:$pass -H 'X-Requested-By: ambari' -X PUT -d \
    '{"RequestInfo": {"context" :"Stop '"$1"' via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' \
    http://$host/api/v1/clusters/$cluster/services/$1
  wait $1 "INSTALLED"
}

function wait(){
  finished=0
  while [ $finished -ne 1 ]
  do
    str=$(curl -s -u $user:$pass http://{$host}/api/v1/clusters/$cluster/services/$1)
    if [[ $str == *"$2"* ]] || [[ $str == *"Service not found"* ]] 
    then
      finished=1
    fi
    sleep 3
  done
}

user='admin'
pass='admin'
cluster='Sandbox'
host='localhost:8080'
zookeeper=localhost:2181
KAFKA_HOME=/usr/hdp/2.2.4.2-2/kafka/bin/
log_dirs=/kafka-logs/


echo "stopping hbase..."
stopWait HBASE

echo "starting hbase..."
startWait HBASE

echo "done!"
