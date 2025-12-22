# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimcurrency"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

currencyDf= spark.table("bronze.currency")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimcurrencyDf = currencyDf.filter(currencyDf.RecordId.isNotNull()
    ).select(
       currencyDf.CurrencyId,
       F.trim(currencyDf.Code).alias("CurrencyCode"),
       F.when(currencyDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(currencyDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(currencyDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       F.trim(currencyDf.Country).alias("Country"),
       F.trim(currencyDf.CurrencyName).alias("CurrencyName"),
       currencyDf.RecordId.alias("CurrencyRecordId"), 
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("CurrencyHashKey", F.xxhash64("CurrencyRecordId")
    )
display(dimcurrencyDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimcurrencyDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatalog(df_final,"silver",Entity)

# COMMAND ----------

