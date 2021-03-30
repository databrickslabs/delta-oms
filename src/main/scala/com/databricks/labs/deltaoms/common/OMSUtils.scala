package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, OMSConfig}
import com.databricks.labs.deltaoms.model.{DatabaseDefinition, TableDefinition}
import org.apache.spark.internal.Logging

trait OMSUtils extends Serializable with Logging with ConfigurationSettings with OMSchemas {

  private val omsVersion = "0.1"
  private val entityName = "oms"
  private val omsProperties = Map("entity" -> s"$entityName", "oms.version" -> s"$omsVersion")

  lazy val omsDBPath = s"${omsConfig.baseLocation}/${omsConfig.dbName}"
  lazy val rawCommitTablePath = s"${omsDBPath}/${omsConfig.rawCommitTable}/"
  lazy val rawActionsTablePath = s"${omsDBPath}/${omsConfig.rawActionTable}/"
  lazy val pathConfigTablePath = s"${omsDBPath}/${omsConfig.pathConfigTable}/"
  lazy val pathSnapshotTablePath = s"${omsDBPath}/${omsConfig.pathSnapshotTable}/"

  val rawCommitPartitions = Seq(PUID,COMMIT_DATE)
  val rawActionsPartitions = Seq(PUID,COMMIT_DATE)


  def pathConfigTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.pathConfigTable,
      omsConfig.dbName,
      pathConfig,
      s"$pathConfigTablePath",
      Some("OMS Path Config Table"),
      omsProperties
    )
  }

  def rawCommitTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.rawCommitTable,
      omsConfig.dbName,
      rawCommit,
      s"$rawCommitTablePath",
      Some("OMS Delta Raw Commit Table"),
      omsProperties,
      rawCommitPartitions
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.rawActionTable,
      omsConfig.dbName,
      rawAction,
      s"$rawActionsTablePath",
      Some("OMS Delta Raw Actions Table"),
      omsProperties,
      rawActionsPartitions)
  }

  def pathSnapshotTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.pathSnapshotTable,
      omsConfig.dbName,
      pathSnapshot,
      s"$pathSnapshotTablePath",
      Some("OMS Path Snapshot Table"),
      omsProperties
    )
  }

  def omsDatabaseDefinition(omsConfig: OMSConfig): DatabaseDefinition = {
    DatabaseDefinition(omsConfig.dbName,
      Some(s"$omsDBPath"),
      Some("OMS Database"),
      omsProperties
    )
  }
}

object OMSUtils extends OMSUtils
