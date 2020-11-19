# Databricks notebook source
# MAGIC %sql
# MAGIC drop database if exists raw cascade;
# MAGIC create database if not exists raw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create a account table to store data
# MAGIC CREATE TABLE IF NOT EXISTS raw.account
# MAGIC   (row_num int,firstName string, lastName string, height int, amount int, insertTimeStamp timeStamp, updateTimeStamp timeStamp)

# COMMAND ----------

from pyspark.sql import *
# import pyspark

# COMMAND ----------

# randomly generate data
import random
import string
i = 1
firstName = ''
lastName = ''
while (i <= 200):
  height = "1" + str(random.randint(6,9)) + str(random.randint(0,9))
  amount = str(random.randint(1,9)) + "0000"
  firstName = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(4,8)))
  lastName = ''.join(random.choice(string.ascii_lowercase) for _ in range(random.randint(4,8)))
  df_2 = spark.sql(f"INSERT INTO raw.account \
  VALUES ({i},\"{firstName}\", \"{lastName}\", {height}, {amount}, current_timestamp(), current_timestamp())")
  i = i + 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- overview account table
# MAGIC select * from raw.account