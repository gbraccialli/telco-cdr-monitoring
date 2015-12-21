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
3. [Observe results](https://github.com/gbraccialli/telco-cdr-monitoring#observe_results) in HDFS, Hive, HBase/Phoenix, Solr/Banana

---------------------

#### Setup demo via scripts on vanilla HDP 2.3.2 sandbox

These setup steps are only needed first time and may take upto 30min to execute (depending on your internet connection)
  - While waiting on any step, if you don't already have Twitter credentials, follow steps [here](https://github.com/hortonworks-gallery/hdp22-twitter-demo#setup-twitter-credentials) to get them

- Download HDP 2.3 sandbox VM image file (Sandbox_HDP_2.3_VMWare.ova) from [Hortonworks website](http://hortonworks.com/products/hortonworks-sandbox/) 
- Import the ova into VMWare Fusion and allocate at least 4cpus and 8GB RAM (its preferable to increase to 9.6GB+ RAM) and start the VM
- Find the IP address of the VM and add an entry into your machines hosts file e.g.
```
192.168.191.241 sandbox.hortonworks.com sandbox    
```
- Connect to the VM via SSH (password hadoop). You can also SSH via browser by clicking: http://sandbox.hortonworks.com:4200
```
ssh root@sandbox.hortonworks.com
```

- **Download code** as root user
```
cd
git clone https://github.com/hortonworks-gallery/hdp22-twitter-demo.git	
```

- **Setup demo**:Run below to setup demo (one time): it will start Ambari/HBase/Kafka/Storm and install maven, solr, banana. 
```
cd /root/hdp22-twitter-demo
./setup-demo.sh
```
  - while it runs, proceed with installing VNC service per steps below

