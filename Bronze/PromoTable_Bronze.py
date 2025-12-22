# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "PromoTable"
EntityPath = f"Sales/{Entity}/"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

PromoTabledf = readFromDeltaPath(EntityPath)
display(PromoTabledf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(PromoTabledf,"Bronze",Entity)

# COMMAND ----------

