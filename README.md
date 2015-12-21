## Telecom - CDR Monitoring Demo

This demo was created to show Streaming components usage in a Telecom company.

Author: Guilherme Braccialli

With special thanks to:
  - [Ali Bajwa](https://github.com/abajwa-hw) who created original Twitter Demo that inspired this demo.
  - [Randy Gelhausen](https://github.com/randerzander) who created Storm Commons component.

------------------

#### Purpose: Monitor Telecom Network equipments analyzing CDR (call detail record) records.

- Ingest: 
  - Listen for CDR records in a directory
- Processing:
  - Monitor dropped calls
  - Monitor networks type change
- Persistence:
  - Hive Streaming ORC (for interactive query) 
  - HBase (for granular events alerts)
  - Solr/Banana (for alerts and reports/dashboards)
- Simulation:
  -  Generate "fake"CDR files every minute 
- Demo setup:
  - Start HDP 2.3.2 sandbox and run provided scripts to setup demo 

------------------
	
#### Contents

1. [Setup demo via scripts on vanilla HDP 2.3.2 sandbox](https://github.com/gbraccialli/telco-cdr-monitoring#setup-demo-via-scripts-on-vanilla-hdp-232-sandbox)
2. [Run demo](https://github.com/gbraccialli/telco-cdr-monitoring#run-demo) to monitor Tweets about S&P 500 securities in realtime
3. [Observe results](https://github.com/gbraccialli/telco-cdr-monitoring#observe-results) in HDFS, Hive, HBase/Phoenix, Solr/Banana

---------------------

#### Setup demo via scripts on vanilla HDP 2.3.2 sandbox

These setup steps are only needed first time and may take upto 30min to execute (depending on your internet connection)

- Download HDP 2.3.2 sandbox from [Hortonworks website](http://hortonworks.com/products/hortonworks-sandbox/) 
- Import the Sandbox into VMWare or VirtualBox and allocate at least 4cpus and 10GB RAM and start the VM
- Connect to the VM via SSH (password hadoop). You can also SSH via browser by clicking: http://sandbox.hortonworks.com:4200
```
ssh -p 2222 root@localhost
```

- **Download code** as root user
```
cd
git clone https://github.com/gbraccialli/telco-cdr-monitoring.git	
```

- **Setup demo**:Run below to setup demo (one time): it will start HBase/Kafka/Storm, install solr, banana and create hive/phoenix tables.
```
cd /root/telco-cdr-monitoring
scripts/setup_demo.sh
```

---------------------

#### Run demo

```
ssh -p 2222 root@localhost
```

- Start all components:
```
cd /root/telco-cdr-monitoring
scripts/start_demo.sh
```
or 
- Start individual compoments:
  - Flume
```
cd /root/telco-cdr-monitoring
scripts/start_flume.sh
```
  - Storm
```
cd /root/telco-cdr-monitoring
scripts/start_storm.sh
```
  - CDR Producer
```
cd /root/telco-cdr-monitoring
scripts/start_cdr_producer.sh
```

---------------------

#### Observe results

- Dashboards
  - [Dashboard Dropped Calls](http://localhost:8983/solr/banana/index.html#/dashboard/file/networktypechange.json)
  - [Dashboard Network Type Change](http://localhost:8983/solr/banana/index.html#/dashboard/file/networktypechange.json)
- Hive/Phoenix tables
  - telco_cdr_monitoring_raw
  - telco_cdr_monitoring_phoenix_dropped_call
  - telco_cdr_monitoring_phoenix_network_type_change
