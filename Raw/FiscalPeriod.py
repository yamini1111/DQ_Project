# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "FiscalPeriod"
manifest = "Others"
deltaLakePath = "DeltaLake/Raw/Others/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

fiscalperioddf = readEntity(manifest,entityName)
display(fiscalperioddf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(fiscalperioddf,deltaLakePath)

# COMMAND ----------

