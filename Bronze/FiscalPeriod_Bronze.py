# Databricks notebook source
# MAGIC %md ###Run Shared Libraries

# COMMAND ----------

# MAGIC %run ../Misc/SharedLibraries

# COMMAND ----------

# MAGIC %md ###Set Variables

# COMMAND ----------

Entity = "FiscalPeriod"
EntityPath = f"Others/{Entity}/"

# COMMAND ----------

# MAGIC %md ###Read From Delta Raw Path

# COMMAND ----------

fiscalperioddf = readFromDeltaPath(EntityPath)
display(fiscalperioddf)


# COMMAND ----------

# MAGIC %md ###Save to Bronze schema

# COMMAND ----------

saveDeltaTableToCatalog(fiscalperioddf,"Bronze",Entity)

# COMMAND ----------

