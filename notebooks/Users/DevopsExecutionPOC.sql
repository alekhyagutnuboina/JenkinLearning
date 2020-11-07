-- Databricks notebook source
-- MAGIC %python
-- MAGIC df=spark.read.format("csv").option("header","true").option("inferschema","true").load('dbfs:/mnt/poc/saitraining/flight-data/csv/2010-summary.csv')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("flights")

-- COMMAND ----------

create table flights
as 
select * from flights

-- COMMAND ----------

