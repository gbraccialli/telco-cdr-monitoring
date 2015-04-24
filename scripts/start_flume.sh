rm -rf /root/cdr/data/*
flume-ng agent -n a1 --f /root/cdr/flume/flume-kafka-cdr.cfg
