# Databricks notebook source
# MAGIC %md ###Run Shared Libraries
# MAGIC

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Define Variables

# COMMAND ----------

entityName = "SalesOrderLine"
manifest = "Sales"
deltaLakePath = "DeltaLake/Raw/Sales/" + entityName

# COMMAND ----------

# MAGIC %md ### Read Entity

# COMMAND ----------

salesorderlinedf = readEntity(manifest,entityName)
display(salesorderlinedf)

# COMMAND ----------

# MAGIC %md ### Write to Delta Lake
# MAGIC

# COMMAND ----------

writeRawToDeltaLake(salesorderlinedf,deltaLakePath)

# COMMAND ----------

