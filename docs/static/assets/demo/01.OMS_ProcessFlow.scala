// Databricks notebook source
// MAGIC %run ./00.Common

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup Test Table and execute different transactions

// COMMAND ----------

val omsSuf = "sep10"

// COMMAND ----------

// MAGIC %md #### Creating Mock Tables

// COMMAND ----------

val testTables = Seq(TableInfo(DBInfo(s"omstestingdb_${omsSuf}_1"),"test_table_1"),
                     TableInfo(DBInfo(s"omstestingdb_${omsSuf}_2"),"test_table_2",Some(Seq("uid"))))

createMockDatabaseAndTables(testTables)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Initialize OMS Environment

// COMMAND ----------

// MAGIC %md #### Define the OMS Database configurations

// COMMAND ----------

val omsBaseLocation = "dbfs:/user/hive/warehouse/oms"
val omsDBName = s"oms_test_${omsSuf}"
val omsCheckpointSuffix = s"_${omsSuf}_12345678"
val omsCheckpointBase = s"$omsBaseLocation/_checkpoints"

// COMMAND ----------

// MAGIC %md
// MAGIC #### Initialize OMS DB and Tables

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.init.InitializeOMS.main(args)

// COMMAND ----------

display(spark.sql(s"show tables in $omsDBName"))

// COMMAND ----------

display(spark.sql(s"describe database $omsDBName"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Update OMS Table Source Config

// COMMAND ----------

// MAGIC %md
// MAGIC #### Adding Mock Databases to the input source configuration

// COMMAND ----------

val testDatabases = testTables.map(_.db).distinct
testDatabases.foreach(x => spark.sql(s"INSERT INTO $omsDBName.sourceconfig VALUES ('${x.dbName}',false, Map('wildCardLevel','0'))"))

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.sourceconfig"))

// COMMAND ----------

// MAGIC %md #Configure Delta table paths into PathConfig from SourceConfig

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.init.ConfigurePaths.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.pathconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingest Raw Delta Logs (with operational metrics)

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName", 
                 s"--checkpointBase=$omsCheckpointBase", 
                 s"--checkpointSuffix=$omsCheckpointSuffix", 
                 "--skipPathConfig",
                 "--skipInitializeOMS")
com.databricks.labs.deltaoms.ingest.StreamPopulateOMS.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.rawactions limit 3"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Process Raw Delta Logs

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.process.OMSProcessRawActions.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.commitinfosnapshots limit 3"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Make Random updates to the Mock tables 

// COMMAND ----------

import scala.util.Random
Random.shuffle(testTables).take(2).foreach(t => spark.sql(s"""UPDATE ${t.db.dbName}.${t.tableName} SET id=2333 WHERE id=878"""))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Retrieve the new Raw Delta Logs (with operational metrics)

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName", 
                 s"--checkpointBase=$omsCheckpointBase", 
                 s"--checkpointSuffix=$omsCheckpointSuffix", 
                 "--skipPathConfig",
                 "--skipInitializeOMS")
com.databricks.labs.deltaoms.ingest.StreamPopulateOMS.main(args)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Process the new Raw Delta Logs

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.process.OMSProcessRawActions.main(args)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Now , we can analyze using the Sample Analysis notebook

// COMMAND ----------

// MAGIC %md
// MAGIC ## (Optional) Advanced way to add wildcard paths to the Source Config

// COMMAND ----------

// MAGIC %md
// MAGIC #### Adding Wildcard directories to the input source configuration

// COMMAND ----------

spark.sql(s"INSERT INTO $omsDBName.sourceconfig VALUES ('dbfs:/databricks-datasets/**',false, Map('wildCardLevel','0'))")
