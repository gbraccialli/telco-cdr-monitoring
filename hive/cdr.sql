add jar /jars/phoenix-hive-4.2.0-jar-with-dependencies.jar;

drop table if exists telco_cdr_monitoring_raw;

create table telco_cdr_monitoring_raw ( 
session_id string,
sim_card_id string,
phone_number string,
record_opening_time string,
duration string,
cell_id string,
network_type string,
drop_reason string
) 
CLUSTERED BY(session_id) INTO 20 BUCKETS
stored as orc tblproperties("orc.compress"="NONE",'transactional'='true')
;

DROP table if exists dropped_call;

CREATE EXTERNAL TABLE dropped_call(
  SIM_CARD_ID string,
  PHONE_NUMBER string,
  RECORD_OPENING_TIME string,
  CELL_ID string,
  DROP_REASON string
)
STORED BY  "org.apache.phoenix.hive.PhoenixStorageHandler"
TBLPROPERTIES(
  'phoenix.hbase.table.name'='cdr.dropped_call',
  'phoenix.zookeeper.znode.parent'='hbase-unsecure',
  'phoenix.rowkeys'='sim_card_id,phone_number,record_opening_time',
  'autocreate'='false',
  'autodrop'='false'
 );

DROP table if exists network_type_change;

CREATE EXTERNAL TABLE network_type_change(
SESSION_ID string,
RECORD_OPENING_TIME string,
CELL_ID_FROM string,
CELL_ID_TO string,
NETWORK_TYPE_CHANGE string
)
STORED BY  "org.apache.phoenix.hive.PhoenixStorageHandler"
TBLPROPERTIES(
  'phoenix.hbase.table.name'='cdr.network_type_change',
  'phoenix.zookeeper.znode.parent'='hbase-unsecure',
  'phoenix.rowkeys'='SESSION_ID, RECORD_OPENING_TIME, CELL_ID_FROM',
  'autocreate'='false',
  'autodrop'='false'
 );
 

drop view if exists dropped_call_view_cdr;
create view dropped_call_view_cdr as 
SELECT 
  sim_card_id, 
  phone_number, 
  previous_cell_id, 
  previous_drop_reason,
  previous_record_opening_time
FROM 
(
   SELECT 
     t1.*,
     LAG(record_opening_time, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestamp ) as previous_record_opening_time,
     LAG(duration, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestamp ) as previous_duration,
     LAG(drop_reason, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestamp ) as previous_drop_reason,
     LAG(cell_id, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestamp ) as previous_cell_id,
     LAG(timestamp, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestamp ) as previous_timestamp
   FROM   
   (
    SELECT cdr.*, unix_timestamp(record_opening_time,'dd/MM/yyyy HH:mm:ss') as timestamp 
    FROM cdr
  ) t1 
) t2
where previous_timestamp is not null and 
cast(timestamp as bigint) <= (cast(previous_timestamp as bigint) + cast(previous_duration as bigint) * 1000L + 60000L) and 
cast(timestamp as bigint) >= (cast(previous_timestamp as bigint) + cast(previous_duration as bigint));


