# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "PurchaseOrder"
EntityPath = f"Purchase/{Entity}/"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

partyAddressdf = readFromDeltaPath(EntityPath)
display(partyAddressdf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(partyAddressdf,"Bronze",Entity)

# COMMAND ----------

