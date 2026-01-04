# Databricks notebook source
catalog_name = "ucdqdev"
ADLS_RAW_PATH = "abfss://operations@sgadlsdqprojectdev.dfs.core.windows.net/DeltaLake/Raw/" 

# COMMAND ----------

def executePrimaryKeyCheck(objectname, layer, rulename, dqattribute1, sqlquery):
    sqlquery_object = sqlquery.replace(objectname, f"{catalog_name}.{layer}.{objectname}")
    df_dqcheck = spark.sql(sqlquery_object)
    if df_dqcheck.isEmpty():           
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname , '{layer}' as sourcelayer , 'PrimaryKeyCheck' as rulename , 1 as sourceresult, current_timestamp() as rundatetime") #rule passed
        display(df_dqcheck_result)
    else:
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{layer}' as sourcelayer , 'PrimaryKeyCheck' as rulename , 0 as sourceresult,current_timestamp() as rundatetime") #rule failed
        
        display(df_dqcheck_result)
        #write bad records
        spark.table(f"{catalog_name}.{layer}.{objectname}").join(df_dqcheck,dqattribute1,"inner").write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/{catalog_name}/dataquality/dqcheckbadrecords/{objectname}/{rulename}/").save()

    #write dq result
    df_dqcheck_result.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/{catalog_name}/dataquality/dqcheckresults/{objectname}/{rulename}/").save()

# COMMAND ----------

def executeNullCheck(objectname, layer, rulename, dqattribute1, sqlquery):
    sqlquery_object = sqlquery.replace(objectname, f"{catalog_name}.{layer}.{objectname}")
    df_dqcheck = spark.sql(sqlquery_object)
    if df_dqcheck.isEmpty():           
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{layer}' as sourcelayer , 'NullCheck' as rulename , 1 as sourceresult, current_timestamp() as rundatetime") #rule passed
        display(df_dqcheck_result)
    else:
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{layer}' as sourcelayer , 'NullCheck' as rulename , 0 as sourceresult,current_timestamp() as rundatetime") #rule failed
        
        display(df_dqcheck_result)
        #write bad records
        df_dqcheck_badrecords = spark.sql(f"SELECT * FROM {catalog_name}.{layer}.{objectname} WHERE {dqattribute1} IS NULL")
        df_dqcheck_badrecords.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/{catalog_name}/dataquality/dqcheckbadrecords/{objectname}/NullCheck/").save()

    #write dq result
    df_dqcheck_result.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/{catalog_name}/dataquality/dqcheckresults/{objectname}/NullCheck/").save()

# COMMAND ----------

def executeRecordCount(domain, objectname, entity, slayer, tlayer, rulename, dqattribute1, dqattribute2, sqlquery):
    
    raw_table = f"delta.`{ADLS_RAW_PATH}{domain}/{objectname}`"
    bronze_table = f"{catalog_name}.bronze.{objectname}"
    silver_table = f"{catalog_name}.silver.{entity}"
    
    if slayer == "raw" and tlayer == "bronze":
        sqlquery_object = (sqlquery.replace("{RAW_TABLE}",raw_table) 
                                    .replace("{BRONZE_TABLE}",bronze_table))
    elif slayer == "bronze" and tlayer == "silver":
        sqlquery_object = (sqlquery.replace("{BRONZE_TABLE}",bronze_table) 
                                    .replace( "{SILVER_TABLE}",silver_table))
        
    df_dqcheck = spark.sql(sqlquery_object)

    if df_dqcheck.isEmpty():
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{slayer}' as sourcelayer, '{tlayer}' as targetlayer , 'RecordCount' as rulename , 1 as sourceresult, current_timestamp() as rundatetime") #rule passed
        display(df_dqcheck_result)
    else:
        df_dqcheck_result = spark.sql(f"SELECT '{objectname}' as objectname, '{slayer}' as sourcelayer, '{tlayer}' as targetlayer , 'RecordCount' as rulename , 0 as sourceresult,current_timestamp() as rundatetime") #rule failed
        display(df_dqcheck_result)
        #write bad records
        
        # df_dqcheck_badrecords.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/{catalog_name}/dataquality/dqcheckbadrecords/{objectname}/RecordCount/").save()

    # #write dq result
    # df_dqcheck_result.write.mode("append").format("csv").option("header","True").option("path",f"/Volumes/{catalog_name}/dataquality/dqcheckresults/{objectname}/RecordCount/").save()

# COMMAND ----------


