# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "Currency"
manifest = "Others"
deltaLakePath = "DeltaLake/Raw/Others/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

currencydf = readEntity(manifest,entityName)
display(currencydf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(currencydf,deltaLakePath)

# COMMAND ----------

