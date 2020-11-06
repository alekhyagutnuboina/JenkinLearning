# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://test@trainingdatablob.blob.core.windows.net",
  mount_point = "/mnt/poc",
  extra_configs = {"fs.azure.account.key.trainingdatablob.blob.core.windows.net":dbutils.secrets.get(scope = "databricksscope", key = "blobkey")})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/poc/saitraining/

# COMMAND ----------

