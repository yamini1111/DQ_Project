# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "PurchCategory"
manifest = "Purchase"
deltaLakePath = "DeltaLake/Raw/Purchase/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

partiesdf = readEntity(manifest,entityName)
display(partiesdf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(partiesdf,deltaLakePath)

# COMMAND ----------

