CREATE TABLE clean_data (

    gender varchar(10),

    seniorcitizen varchar(10),

    partner varchar(10),

    dependents varchar(10),

    phoneservice varchar(10),

    multiplelines varchar(10),

    internetservice varchar(10),

    onlinesecurity varchar(10),

    onlinebackup varchar(10),

    deviceprotection varchar(10),

    techsupport varchar(10),

    streamingtv varchar(10),

    streamingmovies varchar(10),

    tenure NUMERIC(10,2),

    contract varchar(10),

    paperlessbilling varchar(10),

    paymentmethod varchar(10),

    monthlycharges NUMERIC(10,2),

    totalcharges NUMERIC(10,2) ,

    churn NUMERIC(10,2)

);
 
select  count(*) from clean_data
 
 
CREATE TABLE clean_data_new_feature (

    gender varchar(10),

    seniorcitizen varchar(10),

    partner varchar(10),

    dependents varchar(10),

    phoneservice varchar(10),

    multiplelines varchar(10),

    internetservice varchar(10),

    onlinesecurity varchar(10),

    onlinebackup varchar(10),

    deviceprotection varchar(10),

    techsupport varchar(10),

    streamingtv varchar(10),

    streamingmovies varchar(10),

    tenure NUMERIC(10,2),

    contract varchar(10),

    paperlessbilling varchar(10),

    paymentmethod varchar(10),

    monthlycharges NUMERIC(10,2),

    totalcharges NUMERIC(10,2) ,

    churn NUMERIC(10,2),

	all_charges NUMERIC(10,2)

);
 
INSERT INTO clean_data_new_feature (

    gender, seniorcitizen, partner, dependents, phoneservice, multiplelines,

    internetservice, onlinesecurity, onlinebackup, deviceprotection, techsupport,

    streamingtv, streamingmovies, tenure, contract, paperlessbilling, paymentmethod,

    monthlycharges, totalcharges, churn, all_charges

)

SELECT 

    gender, seniorcitizen, partner, dependents, phoneservice, multiplelines,

    internetservice, onlinesecurity, onlinebackup, deviceprotection, techsupport,

    streamingtv, streamingmovies, tenure, contract, paperlessbilling, paymentmethod,

    monthlycharges, totalcharges, churn,

    (monthlycharges * tenure) AS all_charges 

FROM clean_data;
 
ALTER TABLE clean_data_new_feature ADD COLUMN modified_date date
 
alter table clean_data_new_feature drop COLUMN modified_date

 