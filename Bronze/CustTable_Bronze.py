# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "CustTable"
EntityPath = f"Sales/{Entity}/"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

custTableDf = readFromDeltaPath(EntityPath)
display(custTableDf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(custTableDf,"Bronze",Entity)

# COMMAND ----------

