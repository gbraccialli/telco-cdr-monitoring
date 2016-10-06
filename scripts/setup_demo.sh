source /root/telco-cdr-monitoring/scripts/ambari_util.sh

echo '*** Starting Storm....'
startWait STORM

echo '*** Starting HBase....'
startWait HBASE

echo '*** Starting Kafka....'
startWait KAFKA

KAFKA_HOME=/usr/hdp/current/kafka-broker
TOPICS=`$KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --list | grep cdr | wc -l`
if [ $TOPICS == 0 ]
then
	echo "No Kafka topics found...creating..."
	$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic cdr	
fi

mkdir /root/telco-cdr-monitoring/logs/

echo '*** Setup Solr....'
/root/telco-cdr-monitoring/scripts/setup_solr.sh

echo '*** Create Phoenix Tables....'
/usr/hdp/current/phoenix-client/bin/sqlline.py localhost:2181:/hbase-unsecure /root/telco-cdr-monitoring/phoenix/cdr.sql

echo '*** Setup Hive...'
mkdir /usr/hdp/current/hive-client/auxlib
cp /usr/hdp/current/phoenix-client/phoenix-server.jar /usr/hdp/current/hive-client/auxlib/
chmod 755 -R /usr/hdp/current/hive-client/auxlib/

echo '*** Create Hive Tables...'
chmod -R +x /root/
chmod -R +r /root/
sudo -u hive hive -f /root/telco-cdr-monitoring/hive/cdr.sql

