# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "CostCenter"
manifest = "Others"
deltaLakePath = "DeltaLake/Raw/Others/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

costcenterdf = readEntity(manifest,entityName)
display(costcenterdf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(costcenterdf,deltaLakePath)

# COMMAND ----------

