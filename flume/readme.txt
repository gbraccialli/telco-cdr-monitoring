copy /lib/flume-kafka/*.jar to /usr/hdp/2.2.4.2-2/flume/lib/

https://github.com/thilinamb/flume-ng-kafka-sink


flume-ng agent -n a1 --f /root/flume-kafka/flume-ng-kafka-sink/conf/flumeteste.cfg