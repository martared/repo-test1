/*Pre-requisites
drop table if exists smart.imei_stg;
create table smart.imei_stg
(
imeitac varchar,
marketingname varchar,
manufacturer varchar,
band varchar,
brand varchar,
model varchar,
os varchar,
nfc_enabled varchar,
bluetooth_enabled varchar,
wlan_enabled varchar,
device_type varchar,
m2m_or_human2 varchar,
m2m_or_human1 varchar

) distribute by hash(imeitac);

--ncluster_loader  -D ';' -U db_superuser -w db_superuser -d bics --el-discard-errors --el-enabled smart.imei_stg --skip-rows 1 imei1.csv

analyze imei_stg;

*/

---Staging to table
drop table if exists smart.imei;
create table smart.imei distribute by hash(imeitac) as 
select *,
case 
when m2m_or_human2 like '%Human%' or  m2m_or_human2 like '%M2M%' then m2m_or_human2
else m2m_or_human1
end as m2m_or_human
from smart.imei_stg;

analyze  smart.imei;


drop table if exists smart.imei1;
create table smart.imei1 distribute by hash(imeitac) as 
select * from smart.imei where m2m_or_human <> '';

analyze smart.imei1;


---Collecting data
drop table if exists smart.imei_dataset_all;
create table smart.imei_dataset_all distribute by hash(imei) as 
select imsi, imei, substring(imei from 1 for 8) as imei_8,  substring(imei from 1 for 7) as imei_7,  substring(imei from 1 for 6) as imei_6,
 app_thr_id::char(20) as app_thr_id, gi_xdr_protocol_id::char(20) as gi_xdr_protocol_id, gi_xdr_dest_ip::char(20) as gi_xdr_dest_ip, 
 gi_xdr_url_host::char(20) as gi_xdr_url_host, gi_xdr_app_id::char(20) as gi_xdr_app_id , gi_xdr_trans_proto::char(20) as gi_xdr_trans_proto, 
 gi_proto_info_ua::char(20) as gi_proto_info_ua, gi_proto_info_ua_content::char(20) as gi_proto_info_ua_content,
 '3G'::varchar as product
from smart.grx_gtpu_ato   where imei <> '' and imsi <> '' and imsi is not null 
----and call_start_dt = '2016-10-10'
limit 10000000;
;


insert into smart.imei_dataset_all
select imsi, imei, substring(imei from 1 for 8) as imei_8,  substring(imei from 1 for 7) as imei_7,  substring(imei from 1 for 6) as imei_6,
 app_thr_id::char(20) as app_thr_id, gi_xdr_protocol_id::char(20) as gi_xdr_protocol_id, gi_xdr_dest_ip::char(20) as gi_xdr_dest_ip, 
 gi_xdr_url_host::char(20) as gi_xdr_url_host, gi_xdr_app_id::char(20) as gi_xdr_app_id , gi_xdr_trans_proto::char(20) as gi_xdr_trans_proto, 
 gi_proto_info_ua::char(20) as gi_proto_info_ua, gi_proto_info_ua_content::char(20) as gi_proto_info_ua_content,
 '4G'::varchar as product
from smart.ltedr_gtpu_ato  where imei <> ''  and imsi <> '' and imsi is not null 
---and call_start_dt = '2016-10-10'
limit 10000000;


analyze smart.imei_dataset_all ;



---Match 8 characters
drop table if exists smart.imei_dataset_all1;
create table smart.imei_dataset_all1 distribute by hash(imei) as
select *, '8C_MATCH'::varchar as imei_match from smart.imei_dataset_all t1, smart.imei1 t2
where t1.imei_8 = t2.imeitac;

analyze smart.imei_dataset_all1;

--Deleting already matched  8 characters
delete from smart.imei_dataset_all where imei in (select distinct(imei) as imei from smart.imei_dataset_all1);

vacuum smart.imei_dataset_all;
analyze smart.imei_dataset_all;

---Match 7 characters
insert into smart.imei_dataset_all1
select *, '7C_MATCH'::varchar as imei_match from smart.imei_dataset_all t1, smart.imei1 t2
where t1.imei_7 = t2.imeitac;

analyze smart.imei_dataset_all1;

--Deleting already matched  8,7 characters
delete from smart.imei_dataset_all where imei in (select distinct(imei) as imei from smart.imei_dataset_all1);

vacuum smart.imei_dataset_all;
analyze smart.imei_dataset_all;


---Match 6 characters
insert into smart.imei_dataset_all1
select *, '6C_MATCH'::varchar as imei_match from smart.imei_dataset_all t1, smart.imei1 t2
where t1.imei_6 = t2.imeitac;

analyze smart.imei_dataset_all1;

--Deleting already matched  8,7,6 characters
delete from smart.imei_dataset_all where imei in (select distinct(imei) as imei from smart.imei_dataset_all1);

vacuum smart.imei_dataset_all;
analyze smart.imei_dataset_all;



---smart.imei_dataset_all will have remaining
---smart.imei_dataset_all1  will have


--Giving a record number (optional)
drop table if exists smart.imei_dataset_all2;
create table smart.imei_dataset_all2 distribute by hash(imsi) as
--select ROW_NUMBER() OVER (ORDER BY imsi) AS recno,* from smart.imei_dataset_all1;
select * from smart.imei_dataset_all1;

analyze smart.imei_dataset_all2;