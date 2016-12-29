----Building the test and train datasets


DROP TABLE IF EXISTS smart.imei_dataset_test;

CREATE TABLE smart.imei_dataset_test AS
SELECT * FROM smart.imei_dataset_all2 WHERE imsi::numeric %5=0;


analyze smart.imei_dataset_test;




drop table if exists smart.imei_dataset_test_token;
create table smart.imei_dataset_test_token distribute by hash(imsi) as
select imsi, imei, substring(attribute||'-'||regexp_replace(value, ' ', '_') from 1 for 40) as token, m2m_or_human from
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





drop table if exists smart.imei_dataset_test_tfidf;

create table smart.imei_dataset_test_tfidf distribute by hash(imsi) as
select docid as imsi,  term, tf, idf, tf_idf  from tf_idf
(
on tf
(
on (select imsi as docid, token as term from smart.imei_dataset_test_token) 
        partition by docid
) as tf partition by term
on (select count(distinct(imsi)) from smart.imei_dataset_test_token)
as doccount dimension
);

analyze smart.imei_dataset_test_tfidf;


---Joining Back



drop table if exists smart.imei_dataset_test_tfidf1;

create table smart.imei_dataset_test_tfidf1 distribute by hash(imsi) as
select t1.*, t2.imei, t2.m2m_or_human from  
smart.imei_dataset_test_tfidf t1, smart.imei_dataset_test t2
where t1.imsi = t2.imsi;

analyze smart.imei_dataset_test_tfidf1 ;







---SVM Prediction
/*drop table if exists smart.imei_predicted_category_svm;
create table smart.imei_predicted_category_svm distribute by hash(imsi) as
SELECT * FROM SparseSVMPredictor (
ON smart.imei_dataset_test_tfidf1 AS input PARTITION BY imsi
ON smart.imei_sparsesvm_model AS model DIMENSION
SampleIDColumn ('imsi')
AttributeColumn ('term')
ValueColumn ('tf_idf')
AccumulateLabel ('imei', 'm2m_or_human')
) ;*/

---NB Prediction
drop table if exists smart.imei_predicted_category_nb;
create table smart.imei_predicted_category_nb distribute by hash(imsi) as
SELECT * FROM NaiveBayesTextClassifierPredict (
ON (SELECT imsi, token, m2m_or_human as category from 
smart.imei_dataset_test_token) AS predicts PARTITION BY imsi
ON smart.imei_nb_model AS "model" DIMENSION
InputTokenColumn ('token')
ModelType ('Bernoulli')
DocIDColumns ('imsi')
TopK ('1'));

analyze smart.imei_predicted_category_nb;


drop table if exists smart.imei_imsi_test;
create table smart.imei_imsi_test distribute by hash(imsi) as
select imsi, imeitac from smart.imei_dataset_test
group by imsi, imeitac;

analyze smart.imei_imsi_test;



drop table if exists smart.imei_imsi_test;
create table smart.imei_imsi_test distribute by hash(imsi) as
select imsi, imei,m2m_or_human from smart.imei_dataset_test
group by imsi, imei,m2m_or_human;

analyze smart.imei_imsi_test;

drop table if exists smart.imei_predicted_category_nb1;
create table smart.imei_predicted_category_nb1 distribute by hash(imsi) as 
select t1.*, t2.imei, t2.m2m_or_human from smart.imei_predicted_category_nb t1, smart.imei_imsi_test t2 
where t1.imsi = t2.imsi;


insert into app_center_visualizations (json) values (
'{
     "db_table_name":"smart.imei_predicted_category_nb1",
     "vizType":"table",
     "version":"1.0",
     "where" : "",
     "title":"Predicted Results"
}');