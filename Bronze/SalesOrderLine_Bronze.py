# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "SalesOrderLine"
EntityPath = f"Sales/{Entity}/"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

SalesOrderLinedf = readFromDeltaPath(EntityPath)
display(SalesOrderLinedf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(SalesOrderLinedf,"Bronze",Entity)

# COMMAND ----------

