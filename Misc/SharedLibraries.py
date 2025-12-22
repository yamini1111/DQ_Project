# Databricks notebook source
import pyspark.sql.functions as F 
import datetime
import pandas as pd
import dateutil
import json
import pyspark.sql.types as T

# COMMAND ----------

ADLS_DEV_BASE_PATH = "abfss://operations@sgadlsdqprojectdev.dfs.core.windows.net/"
DELTALAKE_RAW_PATH = "DeltaLake/Raw/"

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

spark.sql("USE CATALOG ucdqdev")

# COMMAND ----------


def cdmJsonToSparkSchema(cdm_json_path: str) -> T.StructType:

    cdm_json = json.loads(dbutils.fs.head(cdm_json_path, 500000))
       
    attributes = cdm_json.get('definitions')[0].get('hasAttributes')
    
    fields = []
    for attr in attributes:
        name = attr["name"]
        dtype = attr["dataFormat"]
        
        if dtype == "String":
            spark_type = T.StringType()
        elif dtype == "Int64":
            spark_type = T.LongType()
        elif dtype == "Int32":
            spark_type = T.IntegerType()
        elif dtype == "Boolean":
            spark_type = T.BooleanType()
        elif dtype == "DateTime":
            spark_type = T.TimestampType()
        elif dtype == "Decimal":
            spark_type = T.DecimalType(38, 18)
        else:
            spark_type = T.StringType()  # safe fallback
        
        fields.append(T.StructField(name, spark_type, True))
    
    return T.StructType(fields)

# COMMAND ----------

def readEntity(manifestPath,entity):
    schema = cdmJsonToSparkSchema(f"{ADLS_DEV_BASE_PATH}source/tables/{manifestPath}/{entity}.cdm.json")

    df = (
            spark.read 
            .schema(schema) 
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") 
            .csv(f"{ADLS_DEV_BASE_PATH}source/tables/{manifestPath}/{entity}/")
        )

    return df

# COMMAND ----------

def writeRawToDeltaLake(entityDf,deltaLakePath):
    entityDf.write.mode("overwrite").option("overwriteSchema","True").option("path",ADLS_DEV_BASE_PATH + deltaLakePath).save()

# COMMAND ----------

def readFromDeltaPath(entityName):
  df = (
    spark.read.format("delta")
    .option("path",f"{ADLS_DEV_BASE_PATH}/{DELTALAKE_RAW_PATH}{entityName}")
    .load()
  )
  return df


# COMMAND ----------

def saveDeltaTableToCatalog(df,schema,tableName):
    schema = schema.lower()
    tableName = tableName.lower()
    spark.sql("USE CATALOG ucdqdev")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    df.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.{tableName}")


# COMMAND ----------

# def readEntity(manifestPath,entity):
#     df = (spark.read.format("com.microsoft.cdm")
#     .option("storage", "90111adlsdev.dfs.core.windows.net")
#     .option("appid",appid)
#     .option("appkey",service_credential) 
#     .option("tenantid",tenantid)
#     .option("manifestPath", f"oaonoperationsdev/oaon-sandbox.operations.dynamics.com/Tables/{manifestPath}/{manifestPath}.manifest.cdm.json")
#     .option("entity", entity)
#     #  .option("mode", "permissive")
#     .load())
#     return df
