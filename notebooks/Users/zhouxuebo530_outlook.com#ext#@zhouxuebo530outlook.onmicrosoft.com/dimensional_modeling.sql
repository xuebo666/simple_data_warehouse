-- Databricks notebook source
-- widget factTime
CREATE WIDGET TEXT factTime DEFAULT '2020-10-22T03:02:00.000+0000';

-- COMMAND ----------

-- create fact account table if not exists 
drop table if exists fact_account;
create table if not exists fact_account
(accountID int, amount int, factTime timestamp, updateTimeStamp timestamp);

-- COMMAND ----------

  -- create a table which temporarily store filtered data 
drop table if exists account_test;
create table if not exists account_test
(row_num int, firstName string, lastName string, height int, amount int, changeType string, insertTimeStamp timeStamp, updateTimeStamp timeStamp) using delta;

-- insert data to test table which updateTimeStamp less or equal than fact date
insert into account_test
select f.* from account_target_fl as f
where f.updateTimeStamp <= getArgument("factTime");

-- select data which conform conditions that the lastest data from data source 
select t.* from account_test as t
inner join (select distinct row_num,max(updateTimeStamp) over (partition by row_num) as new_update from account_test group by row_num,updateTimeStamp) as n
on t.row_num = n.row_num AND t.updateTimeStamp = n.new_update and t.changeType <> 'D';

-- COMMAND ----------

-- insert cmd2 data into fact account table
insert into fact_account
select t.row_num,t.amount, cast(getArgument("factTime") as timestamp), current_timestamp() from account_test as t 
inner join (select distinct row_num,max(updateTimeStamp) over (partition by row_num) as new_update from account_test group by row_num,updateTimeStamp) as n
on t.row_num = n.row_num AND t.updateTimeStamp = n.new_update and t.changeType <> 'D';

-- COMMAND ----------

-- dimension table start below

-- COMMAND ----------

-- creat dimension table
drop table if exists dim_account;
create table if not exists dim_account
(accountID int, firstName string, lastName string, height int, validFrom timestamp, validTo timestamp) using delta

-- COMMAND ----------

-- insert data source day1 into dimesion table
insert into dim_account
SELECT row_num, firstName, lastName, height, updateTimeStamp, cast('9999-12-31T23:59:00.000+0000' as timestamp) as validTo
From account_datasource_day1

-- COMMAND ----------

-- create a table to store row number and other information whose data has been changed or deleted
drop table if exists row_num_table; 
create table if not exists row_num_table using delta
select row_num, firstName, lastName, height,changeType, updateTimeStamp from (select f.* from account_target_fl as f
inner join (select row_num from account_target_fl where changeType <> 'I') as s
on s.row_num = f.row_num and f.updateTimeStamp <= getArgument("factTime") and f.changeType <> 'I')

-- COMMAND ----------

-- insert new lines which are new accountIDs
insert into dim_account
select f.row_num, f.firstName, f.lastName, f.height, f.updateTimeStamp, cast('9999-12-31T23:59:00.000+0000' as timestamp) as validTofrom from dim_account as d
right join account_target_fl as f
on d.accountID = f.row_num
where d.accountID is null

-- COMMAND ----------

-- change changetype is D's row's validTo 
merge into dim_account as d
using row_num_table as r
on r.row_num = d.accountID
when matched and r.changeType = 'D' Then update set d.validTo = r.updateTimeStamp

-- COMMAND ----------

-- change changetype is C's row's validTo
merge into dim_account as d
using row_num_table as r
on r.row_num = d.accountID
when matched and r.changeType = 'C' Then update set d.validTo = r.updateTimeStamp

-- COMMAND ----------

-- insert the lastest value which changetype is c
insert into dim_account
SELECT row_num, firstName, lastName, height, updateTimeStamp, cast('9999-12-31T23:59:00.000+0000' as timestamp) as validTo from row_num_table
where changeType = 'C'