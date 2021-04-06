package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.configuration.{ConfigurationSettings, OMSConfig}
import com.databricks.labs.deltaoms.model.{DatabaseDefinition, TableDefinition}
import org.apache.spark.internal.Logging

trait OMSUtils extends Serializable with Logging with ConfigurationSettings with OMSchemas {

  private val omsVersion = "0.1"
  private val entityName = "oms"
  private val omsProperties = Map("entity" -> s"$entityName", "oms.version" -> s"$omsVersion")

  lazy val omsDBPath = s"${omsConfig.baseLocation}/${omsConfig.dbName}"
  lazy val rawActionsTablePath = s"${omsDBPath}/${omsConfig.rawActionTable}/"
  lazy val pathConfigTablePath = s"${omsDBPath}/${omsConfig.pathConfigTable}/"
  lazy val tableConfigPath = s"${omsDBPath}/${omsConfig.tableConfig}/"
  lazy val processedHistoryTablePath = s"${omsDBPath}/${omsConfig.processedHistoryTable}/"

  lazy val commitSnapshotTablePath = s"${omsDBPath}/${omsConfig.commitInfoSnapshotTable}/"
  lazy val commitSnapshotTableName = s"${omsConfig.dbName}.${omsConfig.commitInfoSnapshotTable}"
  lazy val actionSnapshotTablePath = s"${omsDBPath}/${omsConfig.actionSnapshotTable}/"
  lazy val actionSnapshotTableName = s"${omsConfig.dbName}.${omsConfig.actionSnapshotTable}"

  val puidCommitDatePartitions = Seq(PUID,COMMIT_DATE)


  def pathConfigTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.pathConfigTable,
      omsConfig.dbName,
      pathConfig,
      s"$pathConfigTablePath",
      Some("Delta OMS Path Config Table"),
      omsProperties
    )
  }

  def tableConfigDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.tableConfig,
      omsConfig.dbName,
      tableConfig,
      s"$tableConfigPath",
      Some("Delta OMS Table Config"),
      omsProperties
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.rawActionTable,
      omsConfig.dbName,
      rawAction,
      s"$rawActionsTablePath",
      Some("Delta OMS Raw Actions Table"),
      omsProperties,
      puidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(omsConfig: OMSConfig) = {
    TableDefinition(omsConfig.processedHistoryTable,
      omsConfig.dbName,
      processedHistory,
      s"$processedHistoryTablePath",
      Some("Delta OMS Processed History Table"),
      omsProperties
    )
  }

  def omsDatabaseDefinition(omsConfig: OMSConfig): DatabaseDefinition = {
    DatabaseDefinition(omsConfig.dbName,
      Some(s"$omsDBPath"),
      Some("Delta OMS Database"),
      omsProperties
    )
  }
}

object OMSUtils extends OMSUtils
