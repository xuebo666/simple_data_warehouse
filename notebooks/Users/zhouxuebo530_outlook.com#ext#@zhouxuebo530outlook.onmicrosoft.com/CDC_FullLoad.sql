-- Databricks notebook source
-- full load from source system everyday 
-- target table shell catch change type C(change),I(insert),D(delete)
drop table if exists account_target_fl;
create table if not exists account_target_fl
 (row_num int, firstName string, lastName string, height int, amount int, changeType string, insertTimeStamp timeStamp, updateTimeStamp timeStamp) using delta 

-- COMMAND ----------

-- creat day 1 table and insert data from data source
drop table if exists account_datasource_day1;
create table if not exists account_datasource_day1 using delta
select * from raw.account

-- COMMAND ----------

-- initial load
INSERT INTO account_target_fl
SELECT row_num, firstName, lastName, height, amount, "I" as changeType, insertTimeStamp, updateTimeStamp 
From account_datasource_day1

-- COMMAND ----------

-- creat day2 table 
drop table if exists account_datasource_day2;
create table if not exists account_datasource_day2 using delta
select row_num,firstName,lastName,height,amount,current_timeStamp() as insertTimeStamp, current_timeStamp() as updateTimeStamp from raw.account

-- COMMAND ----------

--delete some rows which row_num greater than 180 
delete from account_datasource_day2 where row_num > 180

-- COMMAND ----------

-- insert some new rows into day2 
INSERT INTO account_datasource_day2
VALUES (202,"Xuebo","Zhou",175, 10000, current_timestamp(), current_timestamp()),(203,"xuebo_1","zhou",176, 20000, current_timestamp(), current_timestamp()), (204,"xuebo_2","zhou",177, 30000, current_timestamp(), current_timestamp())

-- COMMAND ----------

-- update data which row_num < 5
UPDATE account_datasource_day2 set height = 190, updateTimeStamp = current_timeStamp() where row_num < 5

-- COMMAND ----------

-- select delete data form day1 insert to delta table
drop table if exists account_delta;
create table if not exists account_delta using delta 
select d1.row_num,d1.firstName,d1.lastName,d1.height,d1.amount,"D" as changeType,d1.insertTimeStamp, current_timeStamp() as updateTimeStamp from account_datasource_day1 as d1
left join account_datasource_day2 as d2 
on d1.row_num = d2.row_num 
where d2.row_num is null

-- COMMAND ----------

-- select new data from day2 
insert into account_delta
select d2.row_num,d2.firstName,d2.lastName,d2.height,d2.amount,"I" as changeType,d2.insertTimeStamp,d2.updateTimeStamp from account_datasource_day1 as d1
right join account_datasource_day2 as d2 
on d1.row_num = d2.row_num 
where d1.row_num is null

-- COMMAND ----------

-- select different data from day2
insert into account_delta
select d2.row_num,d2.firstName,d2.lastName,d2.height,d2.amount,"C" as changeType,d2.insertTimeStamp,d2.updateTimeStamp from account_datasource_day1 as d1
inner join account_datasource_day2 as d2 
on d1.row_num = d2.row_num 
where d1.firstName <> d2.firstName or d1.lastName <> d2.lastName or d1.height <> d2.height 

-- COMMAND ----------

--insert delta table to target table
insert into account_target_fl 
select * from account_delta