# Databricks notebook source
# MAGIC %run ./ADLS_oauth

# COMMAND ----------

df = (spark.read.format("csv")
 .option("path", "abfss://oaonoperationsdev@90111adlsdev.dfs.core.windows.net/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Parties/")
 .load())
display(df)

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")



# COMMAND ----------

df = (spark.read.format("com.microsoft.cdm")
  .option("storage", "90111adlsdev.dfs.core.windows.net")
   .option("appid",appid)
  .option("appkey",service_credential) 
 .option("tenantid",tenantid)
  .option("manifestPath", "oaonoperationsdev/oaon-sandbox.operations.dynamics.com/Tables/Purchase/Purchase.manifest.cdm.json")
  .option("entity", "Parties")
#  .option("mode", "permissive")
  .load())
display(df)

# COMMAND ----------

