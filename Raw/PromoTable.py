# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "PromoTable"
manifest = "Sales"
deltaLakePath = "DeltaLake/Raw/Sales/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

promotabledf = readEntity(manifest,entityName)
display(promotabledf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(promotabledf,deltaLakePath)

# COMMAND ----------

