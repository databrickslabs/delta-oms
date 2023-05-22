// Databricks notebook source
// MAGIC %md
// MAGIC # Use an UC Enabled Cluster

// COMMAND ----------

// MAGIC %md
// MAGIC ## Setup test tables to be monitored by DeltaOMS

// COMMAND ----------

val omsSuf = "may22"
// Set to false if you are using your own test tables
val skipTestDataCreation = false

// COMMAND ----------

val omsTestLocationUrl = s"s3://databricks-deltaoms-testdata/deltaoms_test_${omsSuf}"
val omsTestCatalogName = s"deltaoms_testing_${omsSuf}"
val omsTestSchemaName = s"testing_${omsSuf}"
val wildCardTestPath = s"${omsTestLocationUrl}/${omsTestCatalogName}/${omsTestSchemaName}/**"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating test tables from the `samples` catalog . Refer - https://docs.databricks.com/dbfs/databricks-datasets.html

// COMMAND ----------


if(!skipTestDataCreation) {
  val externalLocationSQL = s"""CREATE EXTERNAL LOCATION IF NOT EXISTS `deltaoms_testing_${omsSuf}_external_location`
  URL '${omsTestLocationUrl}'
  WITH (STORAGE CREDENTIAL `field_demos_credential`)
  COMMENT 'DeltaOMS Testing External Location ${omsSuf}'"""
  spark.sql(externalLocationSQL)

  val testCatalogSQL = s"""
  CREATE CATALOG IF NOT EXISTS ${omsTestCatalogName} 
  MANAGED LOCATION '${omsTestLocationUrl}/${omsTestCatalogName}' 
  COMMENT 'DeltaOMS Test Data Catalog ${omsSuf}'"""
  spark.sql(testCatalogSQL)

  val testSchemaSQL = s"""CREATE SCHEMA IF NOT EXISTS ${omsTestCatalogName}.`${omsTestSchemaName}`
  MANAGED LOCATION '${omsTestLocationUrl}/${omsTestCatalogName}/${omsTestSchemaName}'"""
  spark.sql(testSchemaSQL)
  
  val tripsTableSQL = s"""CREATE TABLE IF NOT EXISTS ${omsTestCatalogName}.`${omsTestSchemaName}`.`trips` 
  AS SELECT * FROM samples.nyctaxi.trips"""
  spark.sql(tripsTableSQL)
  
  val tpchCustomerTableSQL = s"""CREATE TABLE IF NOT EXISTS ${omsTestCatalogName}.`${omsTestSchemaName}`.`tpch_customer` 
  AS SELECT * FROM samples.tpch.customer"""
  spark.sql(tpchCustomerTableSQL)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Initialize and configure DeltaOMS

// COMMAND ----------

// MAGIC %md #### Define the OMS External Location, Catalog, Schema and Tables

// COMMAND ----------

import com.databricks.labs.deltaoms.common.OMSSparkConf._
import org.apache.spark.sql.SparkSession

val omsLocationUrl = s"s3://databricks-deltaoms/deltaoms-${omsSuf}"
val omsLocationName = s"deltaoms-${omsSuf}-external-location"
val storageCredentialName = "field_demos_credential"

val omsCatalogName = s"deltaoms_${omsSuf}"
val omsSchemaName = s"oms_${omsSuf}"
val omsCheckpointBase = s"$omsLocationUrl/__deltaoms/_checkpoints"
val omsCheckpointSuffix = s"_${omsSuf}_123456789"

// Setting the Spark configuration settings for OMS
SparkSession.active.conf.set(LOCATION_URL, omsLocationUrl)
SparkSession.active.conf.set(LOCATION_NAME,omsLocationName)
SparkSession.active.conf.set(STORAGE_CREDENTIAL_NAME,storageCredentialName)
SparkSession.active.conf.set(CATALOG_NAME, omsCatalogName)
SparkSession.active.conf.set(SCHEMA_NAME, omsSchemaName)
SparkSession.active.conf.set(CHECKPOINT_BASE, omsCheckpointBase)
SparkSession.active.conf.set(CHECKPOINT_SUFFIX, omsCheckpointSuffix)

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.init.InitializeOMS.main(args)

// COMMAND ----------

// MAGIC %md #### Validate the DeltaOMS tables were created

// COMMAND ----------

display(spark.sql(s"show tables in $omsCatalogName.$omsSchemaName"))

// COMMAND ----------

display(spark.sql(s"describe extended $omsCatalogName.$omsSchemaName.rawactions"))

// COMMAND ----------

display(spark.sql(s"describe extended $omsCatalogName.$omsSchemaName.commitinfosnapshots"))

// COMMAND ----------

display(spark.sql(s"describe extended $omsCatalogName.$omsSchemaName.actionsnapshots"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Add the data sources (Databases , Catalog, Path, tables) for DeltaOMS monitoring into the `sourceconfig` table

// COMMAND ----------

spark.sql(s"DELETE FROM $omsCatalogName.$omsSchemaName.sourceconfig");
spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES ('${omsTestCatalogName}.`${omsTestSchemaName}`',false)");
spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES ('samples.tpch.orders',false)");

// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (<CATALOG_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (<CATALOG_NAME>.<SCHEMA_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (<CATALOG_NAME>.<SCHEMA_NAME>.<TABLE_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES ('hive_metastore.<SCHEMA_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (CLOUD_STORAGE_PATH/**,false)");

// COMMAND ----------

display(spark.sql(s"select * from $omsCatalogName.$omsSchemaName.sourceconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Configure the wilcard paths to be monitored into the `pathconfig` table

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.init.ConfigurePaths.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsCatalogName.$omsSchemaName.pathconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC # Use a NON-UC Enabled cluster from here
// MAGIC ### For correct results, update Instance Profile role to include all the `wildCardPath`s from the above `PathConfig` table

// COMMAND ----------

// MAGIC %md
// MAGIC ## Ingest Source Delta Logs 
// MAGIC #### (Note : Need to run on a NON-UC Cluster with the appropriate Instance Profile to access the logs under `_delta_log/*.json` for all the wildcard paths)

// COMMAND ----------

import com.databricks.labs.deltaoms.common.OMSSparkConf._
import org.apache.spark.sql.SparkSession
// MODIFY TO MATCH THE ABOVE suffix for DeltaOMS components
val omsSuf = "may22"

// COMMAND ----------

val omsLocationUrl = s"s3://databricks-deltaoms/deltaoms-${omsSuf}"
val omsLocationName = s"deltaoms-${omsSuf}-external-location"
val storageCredentialName = "field_demos_credential"

val omsCatalogName = s"deltaoms_${omsSuf}"
val omsSchemaName = s"oms_${omsSuf}"
val omsCheckpointBase = s"$omsLocationUrl/__deltaoms/_checkpoints"
val omsCheckpointSuffix = s"_${omsSuf}_123456789"

SparkSession.active.conf.set(LOCATION_URL, omsLocationUrl)
SparkSession.active.conf.set(LOCATION_NAME,omsLocationName)
SparkSession.active.conf.set(STORAGE_CREDENTIAL_NAME,storageCredentialName)
SparkSession.active.conf.set(CATALOG_NAME, omsCatalogName)
SparkSession.active.conf.set(SCHEMA_NAME, omsSchemaName)
SparkSession.active.conf.set(CHECKPOINT_BASE, omsCheckpointBase)
SparkSession.active.conf.set(CHECKPOINT_SUFFIX, omsCheckpointSuffix)
SparkSession.active.conf.set(SKIP_PATH_CONFIG, "true")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Start ingesting the available delta transaction logs from the configured wildcard locations

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.ingest.StreamPopulateOMS.main(args)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Validate Delta transaction logs are getting ingested

// COMMAND ----------

val omsTablesBasePath = s"${omsLocationUrl}/${omsCatalogName}/${omsSchemaName}"
display(spark.sql(s"select * from delta.`${omsTablesBasePath}/rawactions` limit 10"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Process Delta Transaction Logs

// COMMAND ----------

// MAGIC %md
// MAGIC #### Process the ingested delta transaction logs from the `rawactions` table into the `commitsnapshots` and `actionsnalshots` tables

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.process.OMSProcessRawActions.main(args)

// COMMAND ----------

display(spark.sql(s"select * from delta.`${omsTablesBasePath}/commitinfosnapshots` limit 10"))

// COMMAND ----------

val tpch_customer_data_path = spark.sql(s"select distinct data_path from delta.`${omsTablesBasePath}/actionsnapshots` where add.stats like '%c_nationkey%' limit 1").as[String].collect()(0)
val tpch_customer_puid = spark.sql(s"select distinct puid from delta.`${omsTablesBasePath}/actionsnapshots` where add.stats like '%c_nationkey%' limit 1").as[String].collect()(0)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Make updates to the `tpch_customer` test table

// COMMAND ----------

spark.sql(s"UPDATE delta.`${tpch_customer_data_path}` SET c_nationkey = 21 where c_custkey = 412446")

// COMMAND ----------

display(spark.sql(s"DESCRIBE HISTORY delta.`${tpch_customer_data_path}`"))

// COMMAND ----------

display(spark.sql(s"select * from delta.`${omsTablesBasePath}/commitinfosnapshots` where puid='${tpch_customer_puid}'"))

// COMMAND ----------

display(spark.sql(s"select * from delta.`${omsTablesBasePath}/actionsnapshots` where puid='${tpch_customer_puid}'"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Retrieve the new Delta transaction Logs into the `rawactions` table

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.ingest.StreamPopulateOMS.main(args)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Process the new transaction logs into `commitsnapshots` and `actionsnapshots`

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.process.OMSProcessRawActions.main(args)

// COMMAND ----------

// MAGIC %md
// MAGIC #### One additional row gets added to the `commitsnapshots` matching the `describe history` command above

// COMMAND ----------

display(spark.sql(s"select * from delta.`${omsTablesBasePath}/commitinfosnapshots`"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Additional row gets added to the `actionsnapshots` for the files added/removed during the new transaction

// COMMAND ----------

display(spark.sql(s"select * from delta.`${omsTablesBasePath}/actionsnapshots` where puid='${tpch_customer_puid}'"))
