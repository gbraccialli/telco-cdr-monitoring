these jars were not released yet. you need either:

A- to copy them to your .m2 local folder before building
	cp ./lib/flume-kafka/*.jar /usr/hdp/2.2.4.2-2/flume/lib/
	cp ./lib/storm-jdbc-0.11.0-SNAPSHOT.jar $M2_HOME/repository/org/apache/storm/storm-jdbc/0.11.0-SNAPSHOT/storm-jdbc-0.11.0-SNAPSHOT.jar
	cp ./lib/StormCommon-1.0-SNAPSHOT.jar $M2_HOME/repository/com/github/randerzander/StormCommon/1.0-SNAPSHOT/StormCommon-1.0-SNAPSHOT.jar

or

B- execute mvn install for projects below:

https://github.com/gbraccialli/StormCommon
https://github.com/apache/storm/tree/master/external/storm-jdbc
	requires: https://github.com/apache/storm/
https://github.com/thilinamb/flume-ng-kafka-sink
