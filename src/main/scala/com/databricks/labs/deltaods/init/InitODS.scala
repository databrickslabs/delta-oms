package com.databricks.labs.deltaods.init

import com.databricks.labs.deltaods.common.ODSSchemas
import com.databricks.labs.deltaods.configuration.ODSConfig
import com.databricks.labs.deltaods.model.{DatabaseDefinition, TableDefinition}
import com.databricks.labs.deltaods.utils.UtilityOperations._
import org.apache.spark.internal.Logging

trait InitODS extends Serializable with Logging {

  private val odsVersion = "0.1"
  private val entityName = "ods"
  private val odsProperties = Map("entity" -> s"$entityName", "ods.version" -> s"$odsVersion")

  def initializeODS(config: ODSConfig) = {
    createODSDB(config)
    createODSTables(config)
  }

  def createODSDB(config: ODSConfig) = {
    logInfo("Creating the ODS Database on Delta Lake")
    createDatabaseIfAbsent(odsDatabaseDefinition(config))
  }

  def odsDatabaseDefinition(odsConfig: ODSConfig): DatabaseDefinition = {
    DatabaseDefinition(odsConfig.dbName,
      Some(odsConfig.baseLocation + "/" + odsConfig.dbName),
      Some("ODS Database"),
      odsProperties
    )
  }

  def createODSTables(config: ODSConfig) = {
    logInfo("Creating the Latest version table on ODS Delta Lake")
    createTableIfAbsent(lastVersionTableDefinition(config))
    logInfo("Creating the Delta Raw Commit table on ODS Delta Lake")
    createTableIfAbsent(rawCommitTableDefinition(config))
  }

  def lastVersionTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.latestVersionTable,
      odsConfig.dbName,
      ODSSchemas.lastVersion,
      s"${odsConfig.baseLocation}/${odsConfig.dbName}/${odsConfig.latestVersionTable}/",
      Some("ODS Last Table Version"),
      odsProperties
    )
  }

  def rawCommitTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.rawCommitTable,
      odsConfig.dbName,
      ODSSchemas.rawCommit,
      s"${odsConfig.baseLocation}/${odsConfig.dbName}/${odsConfig.rawCommitTable}/",
      Some("ODS Delta Raw Commit Table"),
      odsProperties,
      Seq("databaseName", "tableName", "commitDate")
    )
  }

}
