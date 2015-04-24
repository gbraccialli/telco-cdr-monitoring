function start(){
  curl -u $user:$pass -i -H 'X-Requested-By: ambari' -X PUT -d \
    '{"RequestInfo": {"context" :"Start '"$1"' via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' \
    http://$host/api/v1/clusters/$cluster/services/$1
}
 
function startWait(){
  curl -s -u $user:$pass -H 'X-Requested-By: ambari' -X PUT -d \
    '{"RequestInfo": {"context" :"Start '"$1"' via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' \
    http://$host/api/v1/clusters/$cluster/services/$1
  wait $1 "STARTED"
}
 
function stop(){
  curl -u $user:$pass -i -H 'X-Requested-By: ambari' -X PUT -d \
    '{"RequestInfo": {"context" :"Stop '"$1"' via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' \
    http://$host/api/v1/clusters/$cluster/services/$1
}
 
function stopWait(){
  curl -s -u $user:$pass -H 'X-Requested-By: ambari' -X PUT -d \
    '{"RequestInfo": {"context" :"Stop '"$1"' via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' \
    http://$host/api/v1/clusters/$cluster/services/$1
  wait $1 "INSTALLED"
}
 
function maintOff(){
  curl -u $user:$pass -i -H 'X-Requested-By: ambari' -X PUT -d \
  '{"RequestInfo":{"context":"Turn Off Maintenance Mode"},"Body":{"ServiceInfo":{"maintenance_state":"OFF"}}}' \
  http://$host/api/v1/clusters/$cluster/services/$1
}
 
function maintOn(){
  curl -u $user:$pass -i -H 'X-Requested-By: ambari' -X PUT -d \
  '{"RequestInfo":{"context":"Turn Off Maintenance Mode"},"Body":{"ServiceInfo":{"maintenance_state":"ON"}}}' \
  http://$host/api/v1/clusters/$cluster/services/$1
}
 
function delete(){
  curl -u $user:$pass -i -H 'X-Requested-By: ambari' -X DELETE http://$host/api/v1/clusters/$cluster/services/$1
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
 
function check() {
  str=$(curl -s -u $user:$pass http://{$host}/api/v1/clusters/$cluster/services/$1)
  if [[ $str == *"$2"* ]]
  then
    echo 1
  else
    echo 0
  fi
}
 
user='admin'
pass='admin'
cluster='Sandbox'
host='localhost:8080'
 
stopWait KAFKA
startWait KAFKA
