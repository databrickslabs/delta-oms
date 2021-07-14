// Databricks notebook source
// MAGIC %md
// MAGIC ## Initialize OMS Environment

// COMMAND ----------

val omsBaseLocation = "dbfs:/user/hive/warehouse/oms"
val omsDBName = "oms_test_jul07"
val omsCheckpointSuffix = "_jul07_111100"
val omsCheckpointBase = s"$omsBaseLocation/_checkpoints"

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.init.InitializeOMS.main(args)

// COMMAND ----------

display(spark.sql(s"describe database $omsDBName"))

// COMMAND ----------

display(spark.sql(s"show tables in $omsDBName"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Add Input Sources for OMS Tracking

// COMMAND ----------

val dbNames = Seq("tpc_di_bronze","tpc_di_silver","tpc_di_gold")
dbNames.foreach(x => spark.sql(s"INSERT INTO $omsDBName.sourceconfig VALUES ('${x}',false, Map('wildCardLevel','0'))"))

// COMMAND ----------

display(spark.sql(s"SELECT * FROM $omsDBName.sourceconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Configure Paths in OMS for each Input Sources

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.init.ConfigurePaths.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.pathconfig"))
