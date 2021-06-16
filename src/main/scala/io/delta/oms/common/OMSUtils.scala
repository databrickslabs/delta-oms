/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.oms.common

import io.delta.oms.configuration.{ConfigurationSettings, OMSConfig}
import io.delta.oms.model.{DatabaseDefinition, TableDefinition}

import org.apache.spark.internal.Logging

trait OMSUtils extends Serializable with Logging with ConfigurationSettings with OMSchemas {

  lazy val omsDBPath = s"${omsConfig.baseLocation}/${omsConfig.dbName}"
  lazy val rawActionsTablePath = s"${omsDBPath}/${omsConfig.rawActionTable}/"
  lazy val pathConfigTablePath = s"${omsDBPath}/${omsConfig.pathConfigTable}/"
  lazy val sourceConfigTablePath = s"${omsDBPath}/${omsConfig.sourceConfigTable}/"
  lazy val processedHistoryTablePath = s"${omsDBPath}/${omsConfig.processedHistoryTable}/"
  lazy val commitSnapshotTablePath = s"${omsDBPath}/${omsConfig.commitInfoSnapshotTable}/"
  lazy val commitSnapshotTableName = s"${omsConfig.dbName}.${omsConfig.commitInfoSnapshotTable}"
  lazy val actionSnapshotTablePath = s"${omsDBPath}/${omsConfig.actionSnapshotTable}/"
  lazy val actionSnapshotTableName = s"${omsConfig.dbName}.${omsConfig.actionSnapshotTable}"
  val puidCommitDatePartitions = Seq(PUID, COMMIT_DATE)
  private val omsVersion = "0.1"
  private val entityName = "oms"
  private val omsProperties = Map("entity" -> s"$entityName", "oms.version" -> s"$omsVersion")

  def pathConfigTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.pathConfigTable,
      omsConfig.dbName,
      pathConfig,
      s"$pathConfigTablePath",
      Some("Delta OMS Path Config Table"),
      omsProperties
    )
  }

  def sourceConfigDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.sourceConfigTable,
      omsConfig.dbName,
      sourceConfig,
      s"$sourceConfigTablePath",
      Some("Delta OMS Table Config"),
      omsProperties
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.rawActionTable,
      omsConfig.dbName,
      rawAction,
      s"$rawActionsTablePath",
      Some("Delta OMS Raw Actions Table"),
      omsProperties,
      puidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(omsConfig: OMSConfig): TableDefinition = {
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
