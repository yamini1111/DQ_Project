# Databricks notebook source
import pyspark.sql.functions as F 
import datetime
import pandas as pd
import dateutil

# COMMAND ----------

ADLS_DEV_BASE_PATH = "abfss://oaonoperationsdev@90111adlsdev.dfs.core.windows.net/"
DELTALAKE_RAW_PATH = "DeltaLake/Raw/"

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")

spark.conf.set("fs.azure.account.auth.type.90111adlsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.90111adlsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.90111adlsdev.dfs.core.windows.net", appid)
spark.conf.set("fs.azure.account.oauth2.client.secret.90111adlsdev.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.90111adlsdev.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

def readEntity(manifestPath,entity):
    df = (spark.read.format("com.microsoft.cdm")
    .option("storage", "90111adlsdev.dfs.core.windows.net")
    .option("appid",appid)
    .option("appkey",service_credential) 
    .option("tenantid",tenantid)
    .option("manifestPath", f"oaonoperationsdev/oaon-sandbox.operations.dynamics.com/Tables/{manifestPath}/{manifestPath}.manifest.cdm.json")
    .option("entity", entity)
    #  .option("mode", "permissive")
    .load())
    return df

# COMMAND ----------

def writeRawToDeltaLake(entityDf,deltaLakePath):
    entityDf.write.mode("overwrite").option("overwriteSchema","True").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()

# COMMAND ----------

def readFromDeltaPath(entityName):
    df = (spark.read.format("delta")
      .option("path",f"{ADLS_DEV_BASE_PATH}/{DELTALAKE_RAW_PATH}{entityName}")
      .load()
      )
    return df


# COMMAND ----------

def saveDeltaTableToCatalog(df,schema,tableName):
    schema = schema.lower()
    tableName = tableName.lower()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{tableName}")


# COMMAND ----------

