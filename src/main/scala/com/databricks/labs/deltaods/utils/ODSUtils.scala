package com.databricks.labs.deltaods.utils

import com.databricks.labs.deltaods.common.ODSSchemas
import com.databricks.labs.deltaods.configuration.{ConfigurationSettings, ODSConfig}
import com.databricks.labs.deltaods.model.{DatabaseDefinition, TableDefinition}
import org.apache.spark.internal.Logging

trait ODSUtils extends Serializable with Logging with ConfigurationSettings{

  private val odsVersion = "0.1"
  private val entityName = "ods"
  private val odsProperties = Map("entity" -> s"$entityName", "ods.version" -> s"$odsVersion")

  lazy val odsDBPath = s"${odsConfig.baseLocation}/${odsConfig.dbName}"
  lazy val rawCommitTablePath = s"${odsDBPath}/${odsConfig.rawCommitTable}/"
  lazy val rawActionsTablePath = s"${odsDBPath}/${odsConfig.rawActionTable}/"
  lazy val pathConfigTablePath = s"${odsDBPath}/${odsConfig.pathConfigTable}/"

  val rawCommitPartitions = Seq("puid","commitDate")
  val rawActionsPartitions = Seq("puid","commit_date")


  def pathConfigTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.pathConfigTable,
      odsConfig.dbName,
      ODSSchemas.pathConfig,
      s"$pathConfigTablePath",
      Some("ODS Path Config Table"),
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

  def rawActionsTableDefinition(odsConfig: ODSConfig) = {
    TableDefinition(odsConfig.rawActionTable,
      odsConfig.dbName,
      ODSSchemas.rawAction,
      s"$rawActionsTablePath",
      Some("ODS Delta Raw Actions Table"),
      odsProperties,
      rawActionsPartitions)
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
