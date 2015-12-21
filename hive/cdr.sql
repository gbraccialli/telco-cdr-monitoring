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

DROP table if exists telco_cdr_monitoring_phoenix_dropped_call;

CREATE EXTERNAL TABLE telco_cdr_monitoring_phoenix_dropped_call(
  KEY STRUCT<SIM_CARD_ID:string,
            PHONE_NUMBER:string,
            RECORD_OPENING_TIME:string
            >,
  CELL_ID string,
  DROP_REASON string
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '\u0000'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,0:CELL_ID,0:DROP_REASON",
"hbase.table.default.storage.type" = "binary")
TBLPROPERTIES ("hbase.table.name" = "CDR.DROPPED_CALL");


DROP table if exists telco_cdr_monitoring_phoenix_network_type_change;

CREATE EXTERNAL TABLE telco_cdr_monitoring_phoenix_network_type_change(
KEY STRUCT<SESSION_ID:string,
          RECORD_OPENING_TIME:string,
          CELL_ID_FROM:string>,
CELL_ID_TO string,
NETWORK_TYPE_CHANGE string
)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '\u0000'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,0:CELL_ID_TO,0:NETWORK_TYPE_CHANGE",
"hbase.table.default.storage.type" = "binary")
TBLPROPERTIES ("hbase.table.name" = "CDR.NETWORK_TYPE_CHANGE");
 

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
     LAG(record_opening_time, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestampx ) as previous_record_opening_time,
     LAG(duration, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestampx ) as previous_duration,
     LAG(drop_reason, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestampx ) as previous_drop_reason,
     LAG(cell_id, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestampx ) as previous_cell_id,
     LAG(timestampx, 1) OVER (PARTITION BY sim_card_id, phone_number ORDER BY timestampx ) as previous_timestamp
   FROM   
   (
    SELECT cdr.*, unix_timestamp(record_opening_time,'dd/MM/yyyy HH:mm:ss') as timestampx 
    FROM telco_cdr_monitoring_raw cdr
  ) t1 
) t2
where previous_timestamp is not null and 
cast(timestampx as bigint) <= (cast(previous_timestamp as bigint) + cast(previous_duration as bigint) * 1000L + 60000L) and 
cast(timestampx as bigint) >= (cast(previous_timestamp as bigint) + cast(previous_duration as bigint));


