# Databricks notebook source
# MAGIC %run ./ADLS_oauth

# COMMAND ----------

import pyspark.sql.types as T
import json

def cdm_json_to_spark_schema(cdm_json_path: str) -> T.StructType:
    cdm_json = json.loads(dbutils.fs.head(cdm_schema_path, 500000))

        
    attributes = cdm_json.get('definitions')[0].get('hasAttributes')

    print(attributes)
    
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

cdm_schema_path = "abfss://operations@sgadlsdqprojectdev.dfs.core.windows.net/source/tables/Purchase/PurchCategory.cdm.json"
cdm_data_path = "abfss://operations@sgadlsdqprojectdev.dfs.core.windows.net/source/tables/Purchase/PurchCategory"

schema = cdm_json_to_spark_schema(cdm_schema_path)

df = spark.read \
    .schema(schema) \
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") \
    .csv(cdm_data_path)

display(df)


# COMMAND ----------

df = (spark.read.format("csv")
 .option("path", "abfss://operations@sgadlsdqprojectdev.dfs.core.windows.net/source/tables/Purchase/Parties/")
 .load())
display(df)

# COMMAND ----------

# service_credential = dbutils.secrets.get(scope="adbdevscope",key="ClientSecret")
# appid = dbutils.secrets.get(scope="adbdevscope",key="appid")
# tenantid = dbutils.secrets.get(scope="adbdevscope",key="tenantid")



# COMMAND ----------

# # Then run the read code
# df = (spark.read.format("com.microsoft.cdm")

#   .option("storage", "sgadlsdqprojectdev.dfs.core.windows.net")
#    .option("appid",appid)
#   .option("appkey",service_credential) 
#  .option("tenantid",tenantid)
#   .option("manifestPath", "operations/source/tables/Purchase/Purchase.manifest.cdm.json")
#   .option("entity", "Parties")
# #  .option("mode", "permissive")
#   .load())
# display(df)

# COMMAND ----------

# print(spark.conf.get("spark.databricks.service.client.enabled", "false"))
