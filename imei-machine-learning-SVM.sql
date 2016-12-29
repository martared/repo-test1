----Building the test and train datasets

DROP TABLE IF EXISTS smart.imei_dataset_train;


CREATE TABLE smart.imei_dataset_train AS
SELECT * FROM  smart.imei_dataset_all2 WHERE imsi::numeric %5!=0;



analyze smart.imei_dataset_train;



---Create Tokens 
drop table if exists smart.imei_dataset_train_token;
create table smart.imei_dataset_train_token distribute by hash(imsi) as
select imsi, imei, substring(attribute||'-'||regexp_replace(value, ' ', '_') from 1 for 40) as token, m2m_or_human from
Unpivot (
on (select * from smart.imei_dataset_train)
Unpivot(
'app_thr_id', 'gi_xdr_protocol_id', 'gi_xdr_dest_ip', 'gi_xdr_url_host', 'gi_xdr_app_id', 'gi_xdr_trans_proto', 'gi_proto_info_ua', 'gi_proto_info_ua_content'
--'app_thr_id', 'gi_xdr_protocol_id', 'gi_xdr_dest_ip', 'gi_xdr_url_host', 'gi_xdr_app_id', 'gi_xdr_trans_proto', 'gi_proto_info_ua'
)

AttributeColumn ('attribute')
ValueColumn ('value')
InputTypes ('false')
Accumulate ('imsi','imei','m2m_or_human')
) ;

analyze smart.imei_dataset_train_token;




--tf_idf
drop table if exists smart.imei_dataset_train_tfidf;

create table smart.imei_dataset_train_tfidf distribute by hash(imsi) as
select docid as imsi,  term, tf, idf, tf_idf  from tf_idf
(
on tf
(
on (select imsi as docid, token as term from smart.imei_dataset_train_token) 
        partition by docid
) as tf partition by term
on (select count(distinct(imsi)) from smart.imei_dataset_train_token)
as doccount dimension
);

analyze smart.imei_dataset_train_tfidf;





---Joining Back
drop table if exists smart.imei_dataset_train_tfidf1;

create table smart.imei_dataset_train_tfidf1 distribute by hash(imsi) as
select t1.*, t2.imei, t2.m2m_or_human from  
smart.imei_dataset_train_tfidf t1, smart.imei_dataset_train t2
where t1.imsi = t2.imsi;

analyze smart.imei_dataset_train_tfidf1 ;






---SVM Model
/*drop table if exists smart.imei_sparsesvm_model;

SELECT * FROM SparseSVMTrainer (
ON (SELECT 1) PARTITION BY 1
UserID('db_superuser')
Password('db_superuser')
InputTable ('smart.imei_dataset_train_tfidf1')
ModelTable ('smart.imei_sparsesvm_model')
SampleIDColumn ('imsi')
AttributeColumn ('term')
ValueColumn ('tf_idf')
LabelColumn ('m2m_or_human')
MaxStep (100)
Seed ('10')
);*/

---NB Model
drop table if exists smart.imei_nb_model;
create table smart.imei_nb_model distribute by hash(token) as
SELECT * FROM NaiveBayesTextClassifierTrainer (
ON (SELECT * FROM NaiveBayesTextClassifierInternal (
ON (SELECT imsi, token, m2m_or_human as category from 
smart.imei_dataset_train_token) AS "TOKENS" PARTITION BY category
TokenColumn ('token')
ModelType ('Bernoulli')
DocIDColumns ('imsi')
DocCategoryColumn ('category')
)
) PARTITION BY 1) ;





---Vizualization

INSERT INTO app_center_visualizations  (json) 
SELECT json FROM Visualizer (
ON (select * from smart.imei_dataset_train_tfidf1 where m2m_or_human = 'Human' order by tf_idf desc limit 20 ) PARTITION BY 1 
AsterFunction('tfidf') 
Title('Predictor Features for Human') 
VizType('wordcloud')
);



INSERT INTO app_center_visualizations  (json) 
SELECT json FROM Visualizer (
ON (select * from smart.imei_dataset_train_tfidf1 where m2m_or_human = 'M2M' order by tf_idf desc limit 20) PARTITION BY 1 
AsterFunction('tfidf') 
Title('Predictor Features for Human') 
VizType('wordcloud')
);