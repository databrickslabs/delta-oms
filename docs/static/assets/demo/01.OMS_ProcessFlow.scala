// Databricks notebook source
// MAGIC %run ./00.Common

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup Test Table and execute different transactions

// COMMAND ----------

val omsSuf = "sep10"

// COMMAND ----------

/*val testTables = Seq(TableInfo(DBInfo("omstestingdb_jun22_1"),"test_table_1"), 
                     TableInfo(DBInfo("omstestingdb_jun22_1"),"test_table_2"),
                     TableInfo(DBInfo("omstestingdb_jun22_2"),"test_table_3"),
                     TableInfo(DBInfo("omstestingdb_jun22_2"),"test_table_4",Some(Seq("uid"))))*/
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

// DBTITLE 1,Alternate approach to set Configuration using Config file
val oms_config = s"""base-location="$omsBaseLocation"
db-name="$omsDBName"
checkpoint-base="$omsBaseLocation/_checkpoints"
checkpoint-suffix="$omsCheckpointSuffix"
"""
dbutils.fs.put(s"$omsBaseLocation/config/$omsDBName.conf",oms_config,overwrite=true)
System.setProperty("OMS_CONFIG_FILE", s"$omsBaseLocation/config/$omsDBName.conf")

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
// MAGIC ## Update OMS Table Config

// COMMAND ----------

// MAGIC %md
// MAGIC #### Adding Databases to the input source configuration

// COMMAND ----------

val testDatabases = testTables.map(_.db).distinct
testDatabases.foreach(x => spark.sql(s"INSERT INTO $omsDBName.sourceconfig VALUES ('${x.dbName}',false, Map('wildCardLevel','0'))"))

// COMMAND ----------

// MAGIC %fs ls dbfs:/databricks-datasets

// COMMAND ----------

// MAGIC %md
// MAGIC #### Adding Wildcard directories to the input source configuration

// COMMAND ----------

spark.sql(s"INSERT INTO $omsDBName.sourceconfig VALUES ('dbfs:/databricks-datasets/**',false, Map('wildCardLevel','0'))")

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.sourceconfig"))

// COMMAND ----------

// MAGIC %md #Populate Delta table PathConfig from Table Config

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.init.ConfigurePaths.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.pathconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Add input source configuration to the INTERNAL path configuration (Advanced Usage)

// COMMAND ----------

spark.sql(s"""INSERT INTO $omsDBName.pathconfig VALUES (
'dbfs:/databricks-datasets/4/',
substring(sha1('dbfs:/databricks-datasets/4/'), 0, 7), 
'dbfs:/databricks-datasets/*/*/*/*/*/_delta_log/*.json', 
substring(sha1('dbfs:/databricks-datasets/*/*/*/*/*/_delta_log/*.json'), 0, 7), 
Map('wildCardLevel','1'),false,'databricks-datasets-4',0,false,'2021-07-23T19:06:04.933+0000')
""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingest Metrics using OMS

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName", 
                 s"--checkpointBase=$omsCheckpointBase", 
                 s"--checkpointSuffix=$omsCheckpointSuffix", 
                 "--skipPathConfig",
                 // "--skipWildcardPathsConsolidation",
                 "--skipInitializeOMS")
com.databricks.labs.deltaoms.ingest.StreamPopulateOMS.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.rawactions limit 3"))

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.process.OMSProcessRawActions.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsDBName.commitinfosnapshots limit 3"))

// COMMAND ----------

import scala.util.Random
Random.shuffle(testTables).take(2).foreach(t => spark.sql(s"""UPDATE ${t.db.dbName}.${t.tableName} SET id=2333 WHERE id=878"""))

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName", 
                 s"--checkpointBase=$omsCheckpointBase", 
                 s"--checkpointSuffix=$omsCheckpointSuffix", 
                 "--skipPathConfig",
                 "--skipInitializeOMS")
com.databricks.labs.deltaoms.ingest.StreamPopulateOMS.main(args)

// COMMAND ----------

// DBTITLE 1,Table Paths changed since last OMS Refresh
val lastProcessedRawActions = spark.sql(s"select lastVersion from $omsDBName.processedhistory where tableName='rawactions'").as[Long].head()
display(spark.sql(s"""SELECT distinct path FROM table_changes('$omsDBName.rawactions', $lastProcessedRawActions)"""))

// COMMAND ----------

val args = Array(s"--baseLocation=$omsBaseLocation", 
                 s"--dbName=$omsDBName")
com.databricks.labs.deltaoms.process.OMSProcessRawActions.main(args)

// COMMAND ----------

display(spark.sql(s"select distinct path from $omsDBName.rawactions"))

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.data_path, sum(a.add_file.size) as sizeInMBytes 
// MAGIC from
// MAGIC ((select * 
// MAGIC from oms_test_jul22.actionsnapshots 
// MAGIC where data_path like '%databricks-datasets%') a
// MAGIC join
// MAGIC (select data_path,max(commit_version) as max_commit_version 
// MAGIC from oms_test_jul22.actionsnapshots 
// MAGIC where data_path like '%databricks-datasets%'
// MAGIC group by data_path) b
// MAGIC on a.data_path = b.data_path and a.commit_version = b.max_commit_version)
// MAGIC group by a.data_path
// MAGIC order by sizeInMBytes desc

// COMMAND ----------


