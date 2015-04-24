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


if [ $# -ne 1 ] ; then
   echo "You must specify 1 argument: kafka topic"
   exit 1
fi

echo "deleting topic..."
echo "${KAFKA_HOME}/kafka-run-class.sh kafka.admin.DeleteTopicCommand --zookeeper $zookeeper --topic $1"
${KAFKA_HOME}/kafka-run-class.sh kafka.admin.DeleteTopicCommand --zookeeper $zookeeper --topic $1

echo "deleting files..."
echo "rm -rf ${log_dirs}/${1}-*/"
rm -rf ${log_dirs}/${1}-*/

echo "deleting zookeeper entries..."
echo "zookeeper-client rmr /brokers/topics/${1}"
zookeeper-client rmr /brokers/topics/${1}

echo "stopping kafka..."
stopWait KAFKA

echo "starting kafka..."
startWait KAFKA

echo "sleep 5, waiting for brokers to start listening"
sleep 5

echo "creating topic..."
echo "${KAFKA_HOME}/kafka-topics.sh --zookeeper $zookeeper --create --replication-factor 1 --partitions 1 --topic $1"
${KAFKA_HOME}/kafka-topics.sh --zookeeper $zookeeper --create --replication-factor 1 --partitions 1 --topic $1

echo "done!"
