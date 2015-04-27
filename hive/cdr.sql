add jar /jars/phoenix-hive-4.2.0-jar-with-dependencies.jar;

drop table if exists cdr;

create table cdr ( 
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


