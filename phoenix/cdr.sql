drop table if exists CDR.DROPPED_CALL;

create table CDR.DROPPED_CALL(
SIM_CARD_ID varchar,
PHONE_NUMBER varchar,
RECORD_OPENING_TIME varchar,
CELL_ID varchar,
DROP_REASON varchar
constraint my_pk primary key (SIM_CARD_ID, PHONE_NUMBER, RECORD_OPENING_TIME)
) ;


drop table if exists CDR.NETWORK_TYPE_CHANGE;

create table CDR.NETWORK_TYPE_CHANGE(
SESSION_ID varchar,
RECORD_OPENING_TIME varchar,
CELL_ID_FROM varchar,
CELL_ID_TO varchar,
NETWORK_TYPE_CHANGE varchar
constraint my_pk primary key (SESSION_ID, RECORD_OPENING_TIME, CELL_ID_FROM)
) ;

