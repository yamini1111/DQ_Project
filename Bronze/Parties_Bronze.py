# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "Parties"
EntityPath = f"Purchase/{Entity}/"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

partiesdf = readFromDeltaPath(EntityPath)
display(partiesdf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(partiesdf,"Bronze",Entity)

# COMMAND ----------

