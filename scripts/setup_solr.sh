
sudo -u hdfs hadoop fs -mkdir /user/solr
sudo -u hdfs hadoop fs -chown solr /user/solr

cp /root/telco-cdr-monitoring/solr/* /opt/lucidworks-hdpsearch/solr/server/solr-webapp/webapp/banana/app/dashboards/

chown -R solr:solr /opt/lucidworks-hdpsearch/solr

sudo -u solr /opt/lucidworks-hdpsearch/solr/bin/solr start -c -z localhost:2181

sleep 5

sudo -u solr /opt/lucidworks-hdpsearch/solr/bin/solr create -c cdr \
   -d data_driven_schema_configs \
   -s 1 \
   -rf 1
