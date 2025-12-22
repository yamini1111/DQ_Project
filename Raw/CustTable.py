# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "CustTable"
manifest = "Sales"
deltaLakePath = "DeltaLake/Raw/Sales/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

custtabledf = readEntity(manifest,entityName)
display(custtabledf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(custtabledf,deltaLakePath)

# COMMAND ----------

