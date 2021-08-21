# Databricks notebook source
import datetime
date= datetime.datetime.now()
process_dt=date.strftime("%Y-%m-%d")
process_dt_id=int(date.strftime("%Y%m%d%H%M%S"))
print(process_dt)
print(process_dt_id)

# COMMAND ----------

dbutils.widgets.text("filepath","dbfs:/mnt/poc/saitraining/DummyData.xlsx")
dbutils.widgets.text("stagetablename","")

# COMMAND ----------

filepath=dbutils.widgets.get("filepath")
stagetablename=dbutils.widgets.get("stagetablename")

print({'filepath':filepath,'stagetablename':stagetablename,'process_dt':process_dt,'process_dt_id':process_dt_id})



# COMMAND ----------

def landingtostg()