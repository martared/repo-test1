----Building the test and train datasets

DROP TABLE IF EXISTS smart.imei_dataset_train;
DROP TABLE IF EXISTS smart.imei_dataset_test;

CREATE TABLE smart.imei_dataset_train AS
SELECT * FROM  smart.imei_dataset_all2 WHERE imsi%5!=0;

CREATE TABLE smart.imei_dataset_test AS
SELECT * FROM smart.imei_dataset_all2 WHERE imsi%5=0;

analyze smart.imei_dataset_train;
analyze smart.imei_dataset_test;


---Create Tokens 
drop table if exists smart.imei_dataset_train_token;
create table smart.imei_dataset_train_token distribute by hash(imsi) as
select * from
Unpivot (
on (select * from smart.imei_dataset_train)
Unpivot(
'app_thr_id', 'gi_xdr_protocol_id', 'gi_xdr_dest_ip', 'gi_xdr_url_host', 'gi_xdr_app_id', 'gi_xdr_trans_proto', 
'gi_proto_info_ua', 'gi_proto_info_ua_content')
AttributeColumn ('attribute')
ValueColumn ('value')
InputTypes ('false')
Accumulate ('imsi','imei','m2m_or_human')
) ;

analyze smart.imei_dataset_train_token;

drop table if exists smart.imei_dataset_test_token;
create table smart.imei_dataset_test_token distribute by hash(imsi) as
select * from
Unpivot (
on (select * from smart.imei_dataset_test)
Unpivot(
'app_thr_id', 'gi_xdr_protocol_id', 'gi_xdr_dest_ip', 'gi_xdr_url_host', 'gi_xdr_app_id', 'gi_xdr_trans_proto', 
'gi_proto_info_ua', 'gi_proto_info_ua_content')
AttributeColumn ('attribute')
ValueColumn ('value')
InputTypes ('false')
Accumulate ('imsi','imei','m2m_or_human')
) ;

analyze smart.imei_dataset_test_token;



----NB Token Model
drop table if exists smart.imei_token_model;
create table smart.imei_token_model distribute by hash(token) as
SELECT * FROM NaiveBayesTextClassifierTrainer (
ON (SELECT * FROM NaiveBayesTextClassifierInternal (
ON (SELECT imsi, attribute||'-'||value  as token, m2m_or_human as category from 
smart.imei_dataset_train_token) AS "TOKENS" PARTITION BY category
TokenColumn ('token')
ModelType ('Bernoulli')
DocIDColumns ('imsi')
DocCategoryColumn ('category')
)
) PARTITION BY 1) ;

select * from smart.imei_token_model;


--Prediction

drop table if exists smart.imei_prediction;
create table smart.imei_prediction distribute by hash(imsi) as
SELECT * FROM NaiveBayesTextClassifierPredict (
ON (SELECT imsi, attribute||'-'||value  as token, m2m_or_human as category from 
smart.imei_dataset_test_token) AS predicts PARTITION BY recno
ON smart.imei_token_model AS "model" DIMENSION
InputTokenColumn ('token')
ModelType ('Bernoulli')
DocIDColumns ('imsi')
TopK ('1'));

analyze smart.imei_prediction;


--Results
drop table if exists smart.imei_prediction1;
create table  smart.imei_prediction1 distribute  by hash(imei) as 
select t1.imsi, t1.imei, t1.m2m_or_human, t2.prediction, t2.loglik  from smart.imei_dataset_test t1, smart.imei_prediction t2
where t1.imsi = t2.imsi;

analyze smart.imei_prediction1;/*drop table if exists smart.imei_prediction2;
create table smart.imei_prediction2 distribute by hash(imei) as
select imei, m2m_or_human, prediction, avg(loglik) as loglik 
from smart.imei_prediction1
group by imei, m2m_or_human, prediction;

analyze smart.imei_prediction2;*/