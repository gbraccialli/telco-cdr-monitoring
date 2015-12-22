scripts/stop_demo.sh

source /root/telco-cdr-monitoring/scripts/ambari_util.sh

echo '*** Starting Storm....'
startWait STORM

echo '*** Starting HBase....'
startWait HBASE

echo '*** Starting Kafka....'
startWait KAFKA

echo '*** Starting Solr....'
sudo -u solr /opt/lucidworks-hdpsearch/solr/bin/solr start -c -z localhost:2181

echo '*** Starting Demo Flume....'
/root/telco-cdr-monitoring/scripts/start_flume.sh > /root/telco-cdr-monitoring/logs/flume.log 2>&1 &
echo "check logs at /root/telco-cdr-monitoring/logs/flume.log"

echo '*** Starting Demo Topology....'
/root/telco-cdr-monitoring/scripts/start_storm.sh > /root/telco-cdr-monitoring/logs/storm.log 2>&1 &
echo "check logs at /root/telco-cdr-monitoring/logs/storm.log"

echo '*** Starting Demo Producer....'
/root/telco-cdr-monitoring/scripts/start_cdr_producer.sh > /root/telco-cdr-monitoring/logs/producer.log 2>&1 &
echo "check logs at /root/telco-cdr-monitoring/logs/producer.log"