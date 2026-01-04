# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../01_Misc/01_SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimpurchasecontracts"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

purchaseContractDf= spark.table("bronze.purchcontracts")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimpurchaseContractDf = purchaseContractDf.filter(purchaseContractDf.RecordId.isNotNull()
    ).select(
       purchaseContractDf.ContractId,
       F.when(purchaseContractDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(purchaseContractDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       F.from_utc_timestamp(purchaseContractDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        F.trim(purchaseContractDf.FirstParty).alias("FirstParty"),
        F.trim(purchaseContractDf.SecondParty).alias("SecondParty"),
        F.from_utc_timestamp(purchaseContractDf.ValidFrom,'CST').alias("ValidFrom"),
        F.from_utc_timestamp(purchaseContractDf.ValidTo,'CST').alias("ValidTo"),
        purchaseContractDf.IsActive,
       purchaseContractDf.RecordId.alias("PurchContractRecordId"),       
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PurchContractHashKey", F.xxhash64("PurchContractRecordId")
    )
display(dimpurchaseContractDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimpurchaseContractDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatalog(df_final,"silver",Entity)

# COMMAND ----------

saveUCsilverTableToDQ('silver','Purchase','PurchContracts',Entity)
