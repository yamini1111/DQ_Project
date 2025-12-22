# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

UpdatedDateTime = datetime.datetime.now()
Entity = "dimparty"

# COMMAND ----------

# MAGIC %md ###Read Bronze tables
# MAGIC

# COMMAND ----------

partiesDf= spark.table("bronze.parties")
partyaddressDf = spark.table("bronze.partyaddress")


# COMMAND ----------

# MAGIC %md ###Build Dimension/Fact table
# MAGIC

# COMMAND ----------

dimPartyDf = partiesDf.join(
    partyaddressDf, partiesDf.PartyId == partyaddressDf.PartyNumber, "left"
    ).filter(partiesDf.RecordId.isNotNull()
    ).select(
        partiesDf.PartyId,
        F.trim(partiesDf.PartyName).alias("PartyName"),
        F.when(partiesDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(partiesDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        F.from_utc_timestamp(partiesDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        F.trim(partiesDf.PartyAddressCode).alias("PartyAddressCode"),
        F.from_utc_timestamp(partiesDf.EstablishedDate,'CST').alias("EstablishedDate"),
        F.trim(partiesDf.PartyEmailId).alias("PartyEmailId"),
        F.trim(partiesDf.PartyContactNumber).alias("PartyContactNumber"),
        partiesDf.RecordId.alias("PartyRecordId"),
        F.trim(partiesDf.TaxId).alias("TaxId"),
        F.trim(partyaddressDf.Address).alias("Address"),
        F.trim(partyaddressDf.City).alias("City"),
        F.trim(partyaddressDf.State).alias("State"),
        F.trim(partyaddressDf.Country).alias("Country"),
        F.trim(partyaddressDf.Region).alias("Region"),
        F.from_utc_timestamp(partyaddressDf.ValidFrom,'CST').alias("ValidFrom"),
        F.when(partyaddressDf.ValidTo.isNull(), "1900-01-01").otherwise(partyaddressDf.ValidTo).cast("timestamp").alias("ValidTo"),
        partyaddressDf.RecordId.alias("PartyAddressRecordId")
    ).withColumn("UpdatedDateTime", F.lit(UpdatedDateTime)
    ).withColumn("PartyHashKey", F.xxhash64("PartyRecordId")
    )
display(dimPartyDf)

# COMMAND ----------

# MAGIC %md ###Final dataframe
# MAGIC

# COMMAND ----------

df_final = dimPartyDf

# COMMAND ----------

# MAGIC %md ## Write to Silver Schema

# COMMAND ----------

saveDeltaTableToCatalog(df_final,"silver",Entity)

# COMMAND ----------

