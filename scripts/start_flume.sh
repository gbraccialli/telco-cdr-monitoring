rm -rf /root/telco-cdr-monitoring/data/*
flume-ng agent -n a1 --f /root/telco-cdr-monitoring/flume/flume-kafka-cdr.cfg
