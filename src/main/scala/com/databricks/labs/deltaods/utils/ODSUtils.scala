package com.databricks.labs.deltaods.utils

import com.databricks.labs.deltaods.common.{ConfigurationSettings, ODSSchemas}
import com.databricks.labs.deltaods.configuration.ODSConfig
import com.databricks.labs.deltaods.model.{DatabaseDefinition, TableDefinition}
import org.apache.spark.internal.Logging

trait ODSUtils extends Serializable with Logging with ConfigurationSettings{

  private val odsVersion = "0.1"
  private val entityName = "ods"
  private val odsProperties = Map("entity" -> s"$entityName", "ods.version" -> s"$odsVersion")
  private val rawCommitPartitions = Seq("qualifiedName","commitDate")

  lazy val odsDBPath = s"${odsConfig.baseLocation}/${odsConfig.dbName}"
  lazy val lastVersionTablePath = s"${odsDBPath}/${odsConfig.latestVersionTable}/"
  lazy val rawCommitTablePath = s"${odsDBPath}/${odsConfig.rawCommitTable}/"
  lazy val pathConfigTablePath = s"${odsDBPath}/${odsConfig.pathConfigTable}/"


  def pathConfigTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.pathConfigTable,
      odsConfig.dbName,
      ODSSchemas.pathConfig,
      s"$pathConfigTablePath",
      Some("ODS Path Config Table"),
      odsProperties
    )
  }

  def lastVersionTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.latestVersionTable,
      odsConfig.dbName,
      ODSSchemas.lastVersion,
      s"$lastVersionTablePath",
      Some("ODS Last Table Version"),
      odsProperties
    )
  }

  def rawCommitTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.rawCommitTable,
      odsConfig.dbName,
      ODSSchemas.rawCommit,
      s"$rawCommitTablePath",
      Some("ODS Delta Raw Commit Table"),
      odsProperties,
      rawCommitPartitions
    )
  }

  def odsDatabaseDefinition(odsConfig: ODSConfig): DatabaseDefinition = {
    DatabaseDefinition(odsConfig.dbName,
      Some(s"$odsDBPath"),
      Some("ODS Database"),
      odsProperties
    )
  }
}

object ODSUtils extends ODSUtils
