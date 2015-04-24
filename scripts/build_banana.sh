/bin/cp -f /root/cdr/solr/solrconfig.xml /opt/solr/latest/hdp/solr/cdr/conf/
/bin/cp -f /root/cdr/solr/*.json /opt/banana/latest/src/app/dashboards/
/bin/cp -f /root/cdr/solr/config.js /opt/banana/latest/src/config.js
rm -f /opt/banana/latest/build/*
cd /opt/banana/latest
ant
/bin/cp -f /opt/banana/latest/build/banana*.war /opt/solr/latest/hdp/webapps/banana.war
/bin/cp -f /opt/banana/latest/jetty-contexts/banana-context.xml /opt/solr/latest/hdp/contexts/

cd -

#solr home
#/opt/solr/latest/hdp

/root/cdr/scripts/restart_erase_solr.sh
