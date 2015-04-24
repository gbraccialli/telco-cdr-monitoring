#stop solr
ps -ef | grep sol[r] | awk '{print $2}' | sudo xargs kill

#erase solr hdfs
hdfs dfs -rmr /user/solr/cdr/*

/root/cdr/scripts/start_solr.sh
