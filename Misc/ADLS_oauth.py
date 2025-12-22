# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

spark.conf.set("fs.azure.account.auth.type.sgadlsdqprojectdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sgadlsdqprojectdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sgadlsdqprojectdev.dfs.core.windows.net", appid)
spark.conf.set("fs.azure.account.oauth2.client.secret.sgadlsdqprojectdev.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sgadlsdqprojectdev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://operations@sgadlsdqprojectdev.dfs.core.windows.net/source/"))

# COMMAND ----------


