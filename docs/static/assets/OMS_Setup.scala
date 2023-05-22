// Databricks notebook source
// MAGIC %md
// MAGIC ## Initialize OMS Environment

// COMMAND ----------

// MAGIC %md
// MAGIC #### Modify/Update the below variables according to your environment

// COMMAND ----------

import com.databricks.labs.deltaoms.common.OMSSparkConf._
import org.apache.spark.sql.SparkSession

val omsSuf = "may22_1"
val omsLocationUrl = s"s3://databricks-deltaoms/deltaoms-${omsSuf}"
val omsLocationName = s"deltaoms-${omsSuf}-external-location"
val storageCredentialName = "field_demos_credential"

val omsCatalogName = s"deltaoms_${omsSuf}"
val omsSchemaName = s"oms_${omsSuf}"

// Setting the Spark configuration settings for OMS
SparkSession.active.conf.set(LOCATION_URL, omsLocationUrl)
SparkSession.active.conf.set(LOCATION_NAME,omsLocationName)
SparkSession.active.conf.set(STORAGE_CREDENTIAL_NAME,storageCredentialName)
SparkSession.active.conf.set(CATALOG_NAME, omsCatalogName)
SparkSession.active.conf.set(SCHEMA_NAME, omsSchemaName)

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.init.InitializeOMS.main(args)

// COMMAND ----------

display(spark.sql(s"show tables in $omsCatalogName.$omsSchemaName"))

// COMMAND ----------

display(spark.sql(s"describe extended $omsCatalogName.$omsSchemaName.commitinfosnapshots"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Add Input Sources for OMS Tracking

// COMMAND ----------

// MAGIC %md
// MAGIC #### Add the data sources (Databases , Catalog, Path, tables) for DeltaOMS monitoring into the `sourceconfig` table

// COMMAND ----------

spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES ('samples.tpch.orders',false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (<CATALOG_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (<CATALOG_NAME>.<SCHEMA_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (<CATALOG_NAME>.<SCHEMA_NAME>.<TABLE_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES ('hive_metastore.<SCHEMA_NAME>,false)");
// spark.sql(s"INSERT INTO $omsCatalogName.$omsSchemaName.sourceconfig VALUES (CLOUD_STORAGE_PATH/**,false)");

// COMMAND ----------

display(spark.sql(s"SELECT * FROM $omsCatalogName.$omsSchemaName.sourceconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Configure Paths in OMS for each Input Sources

// COMMAND ----------

val args = Array.empty[String]
com.databricks.labs.deltaoms.init.ConfigurePaths.main(args)

// COMMAND ----------

display(spark.sql(s"select * from $omsCatalogName.$omsSchemaName.pathconfig"))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now, follow the instructions in the document (https://databrickslabs.github.io/delta-oms/) to create the DeltaOMS Ingestion and Processing jobs to start collecting the Delta transaction logs with operation metrics into the centralized OMS schema/tables
