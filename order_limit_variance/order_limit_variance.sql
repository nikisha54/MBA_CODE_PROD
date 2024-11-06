use ${hiveconf:PROCESSING_SCHEMA};

drop table if exists order_limit_variance;
create table order_limit_variance as
SELECT 
chdactid as account_id,
chdshpid as shipto_id,
ordered_by,
order_date,
order_id,
order_total,
patient_id,
COALESCE(patient_last_name,'NA') AS patient_last_name,
COALESCE(patient_first_name,'NA') AS patient_first_name,
approved_by,
approved_date,
rule_transaction_reason,
chdplat,
ROLLING_DAYS,
ROLLING_TOTAL,
DOLLAR_LIMIT,
  CASE
    WHEN ROLLING_TOTAL IS NOT NULL AND ROLLING_DAYS IS NOT NULL THEN 'Rolling Dollar Violation'
    WHEN ROLLING_DAYS IS NOT NULL AND ROLLING_TOTAL IS NULL THEN 'Rolling Qty Violation'
    WHEN ROLLING_TOTAL IS NULL AND ROLLING_DAYS IS NULL AND INSTR(rule_transaction_reason, 'less') > 0 THEN 'Order Max $ Violation'
    WHEN ROLLING_TOTAL IS NULL AND ROLLING_DAYS IS NULL AND INSTR(rule_transaction_reason, 'more') > 0 THEN 'Order Min $ Violation'
    ELSE NULL
  END AS  Rule_Violation
FROM
(
SELECT 
chdactid,
chdshpid,
concat (split(usrname,' ')[1],' ',split(usrname,' ')[0]) ordered_by,                  
chdcreated order_date,                  
chdorderid order_id,                  
chdtotal order_total, 
chdpntcode patient_id,
chdpntlname patient_last_name,                 
chdpntfname patient_first_name,                  
prtapprovedby approved_by,                 
prtaccepted approved_date,                 
prtreason rule_transaction_reason,
chdplat,
CAST(REGEXP_EXTRACT(prtreason, 'in ([0-9]+) days') as BIGINT) AS rolling_days,
CAST(REGEXP_EXTRACT(prtreason, 'ordered \\$([0-9,.]+)\\.') as DOUBLE) AS rolling_total,
CASE 
WHEN prtreason like '%Orders must be less than%' THEN CAST(REGEXP_EXTRACT(prtreason, 'Orders must be less than \\$([0-9,.]+)') as DOUBLE)
WHEN prtreason like '%Orders must be more than%' THEN CAST(REGEXP_EXTRACT(prtreason, 'Orders must be more than \\$([0-9,.]+)') as DOUBLE)
ELSE CAST(REGEXP_EXTRACT(prtreason, 'Limit \\$([0-9,.]+)') as DOUBLE) END AS dollar_limit
from purchaseruletransaction                  
left join purchaserule on prtprlid = prlid                  
left join completedheader on prtwhdid = chdid                  
left join userprofile on chdusridcreated = usrid and chdplat = usrplat                 
where (prtreason LIKE 'Financial%' OR prtreason LIKE 'Total Orders%')
and prlcriteriatype = 'E'                  
AND chdplat = 'M'                  
and chdorderid is not null                 
and prtapprovedby is not null) as a;

