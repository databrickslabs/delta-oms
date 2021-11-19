// Databricks notebook source
// MAGIC %md
// MAGIC ## Initialize OMS Environment

// COMMAND ----------

// MAGIC %md
// MAGIC #### Update the below variables `omsBaseLocation` and `omsDBName` according to your environment

// COMMAND ----------

val omsBaseLocation = "dbfs:/user/hive/warehouse/oms"
val omsDBName = "oms_test_sep1"

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

// MAGIC %md
// MAGIC #### The `dbNames` variable is a list of databases you want to track/monitor through DeltaOMS. Change the values to your database names

// COMMAND ----------

val dbNames = Seq("tpc_di_bronze","tpc_di_silver","tpc_di_gold")
dbNames.foreach(x => spark.sql(s"INSERT INTO $omsDBName.sourceconfig VALUES ('${x}',false, Map('wildCardLevel','0'))"))

// COMMAND ----------

display(spark.sql(s"SELECT * FROM $omsDBName.sourceconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Configure Paths in OMS for each Input Sources

// COMMAND ----------

// MAGIC %md
// MAGIC #### This populates the internal DeltaOMS configuration table with metadata from your Delta tables in the databases configured above via `sourceconfig`

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.init.ConfigurePaths.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.pathconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Now, follow the instructions in the document (https://databrickslabs.github.io/delta-oms/) to create the DeltaOMS Ingestion and Processing jobs to start collecting the Delta logs with operation metrics

// COMMAND ----------


