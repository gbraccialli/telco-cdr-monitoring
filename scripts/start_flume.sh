rm -rf /root/telco-cdr-monitoring/data/*
flume-ng agent -n cdr_demo --f /root/telco-cdr-monitoring/flume/flume-kafka-cdr.cfg
