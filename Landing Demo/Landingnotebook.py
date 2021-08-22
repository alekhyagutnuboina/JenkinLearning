# Databricks notebook source
import datetime
import os
from multiprocessing import Pool
date= datetime.datetime.now()
process_dt=date.strftime("%Y-%m-%d")
process_dt_id=int(date.strftime("%Y%m%d%H%M%S"))
print(process_dt)
print(process_dt_id)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("filepath","dbfs:/mnt/poc/saitraining/DummyData.xlsx")
dbutils.widgets.text("stagetablename","")
dbutils.widgets.text("file_nm","Customers")

# COMMAND ----------

filepath=dbutils.widgets.get("filepath")
stagetablename=dbutils.widgets.get("stagetablename")
file_name=dbutils.widgets.get("file_nm")

print({'filepath':filepath,'stagetablename':stagetablename,'file_name':file_name,'process_dt':process_dt,'process_dt_id':process_dt_id})



# COMMAND ----------

def landingtostg(filepath,stagetablename):
    df=spark.sql("select * from edw_config.induction_files where file_name='{0}'".format(file_name)).collect()
    file_name=df[0]['file_name']
    stg_db=df[0]['to_db']
    delimiter=df[0]['delimter']
    path=os.path.join(os.path.dirname(filepath),'landing',file_name)
    print(path)                 
    df=spark.read.format('CSV').option("delimiter",'|').option("inferSchema",True).load(path)
    df=spark.read.format('csv').option("inferSchema",True).option("delimiter",'|').load('dbfs:/mnt/poc/landing/Customers.csv').withColumn('filepath',lit(path))\
       .withColumn('filedt',lit(process_dt)).withColumn('process_dt',lit(''))\
          .withColumn('induction_process_dt_id',lit(process_dt_id))\
               .withColumn('row_status_cd',lit('N')).withColumn('issues_list',lit(''))
  
    
                    
                      
    
    
   
  

# COMMAND ----------

df=spark.sql("select * from edw_config.induction_files where stgtblname='{0}'".format('CUSTOMER_DIM')).collect()
df[0]['']


