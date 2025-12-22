# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimvendor"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

vendorDf= spark.table("bronze.VendTable")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimvendorDf = vendorDf.filter(vendorDf.RecordId.isNotNull()
    ).select(
        vendorDf.VendId.alias("VendorId"),
        F.trim(vendorDf.VendorName).alias("VendorName"),
        F.when(vendorDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(vendorDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(vendorDf.ValidFrom,'CST').alias("DataLakeModified_DateTime"),
        F.trim(vendorDf.Address).alias("Address"),
        F.trim(vendorDf.City).alias("City"),
        F.trim(vendorDf.Country).alias("Country"),
        F.trim(vendorDf.ZipCode).alias("ZipCode"),
        F.trim(vendorDf.Region).alias("Region"),
        F.from_utc_timestamp(vendorDf.ValidFrom,'CST').alias("ValidFrom"),
        F.from_utc_timestamp(vendorDf.ValidTo,'CST').alias("ValidTo"),
        vendorDf.Active,
        vendorDf.RecordId.alias("VendorRecordId"),
        F.trim(vendorDf.TaxId).alias("TaxId"),
        F.trim(vendorDf.CurrencyCode).alias("CurrencyCode"),
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("VendorHashKey", F.xxhash64("VendorRecordId")
    ).withColumn("VendorDiscount",F.when(F.col("Country") == "US",F.lit(0.01)).when(F.col("Country") == "UK",F.lit(0.006)).otherwise(F.lit(0))
    )
display(dimvendorDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimvendorDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatalog(df_final,"silver",Entity)

# COMMAND ----------

