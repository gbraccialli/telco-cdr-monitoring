echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"
echo "     starting maven clean install"
echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"

mvn clean package

echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"
echo "     maven finished"
echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"

echo ""

echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"
echo "     copying jar to sandbox"
echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"

#ssh root@sandbox mkdir /root/telco-cdr-monitoring/
scp target/telco-cdr-monitoring-1.0-SNAPSHOT.jar root@sandbox:/root/telco-cdr-monitoring/
scp -r conf/ root@sandbox:/root/telco-cdr-monitoring/
scp -r solr/ root@sandbox:/root/telco-cdr-monitoring/
scp -r flume/ root@sandbox:/root/telco-cdr-monitoring/
scp -r scripts/ root@sandbox:/root/telco-cdr-monitoring/
scp -r hive/ root@sandbox:/root/telco-cdr-monitoring/
scp -r phoenix/ root@sandbox:/root/telco-cdr-monitoring/
scp -r data/ root@sandbox:/root/telco-cdr-monitoring/
ssh root@sandbox chmod +x /root/telco-cdr-monitoring/scripts/*
echo ""

echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"
echo "     copied"
echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"


echo ""

echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"
echo "     starting storm"
echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"

#ssh root@sandbox storm jar /root/telco-cdr-monitoring/telco-cdr-monitoring-1.0-SNAPSHOT.jar com.github.gbraccialli.telco.cdr.storm.Topology /root/telco-cdr-monitoring/conf/topology.props


echo ""

echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"
echo "     end"
echo "      ----------------------------------------------------------------------"
echo "      ----------------------------------------------------------------------"