/*
 * Copyright (2021) Databricks, Inc.
 *
 * Delta Operational Metrics Store(DeltaOMS)
 *
 * Copyright 2021 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.configuration.OMSConfig
import com.databricks.labs.deltaoms.model.{DatabaseDefinition, TableDefinition}

import org.apache.spark.internal.Logging

trait Utils extends Serializable with Logging with Schemas {

  def getOMSDBPath(config: OMSConfig): String =
    s"${config.baseLocation.get}/${config.dbName.get}"
  def getRawActionsTableName(config: OMSConfig): String =
    s"${config.dbName.get}.${config.rawActionTable}"
  def getRawActionsTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.rawActionTable}/"
  def getPathConfigTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.pathConfigTable}/"
  def getSourceConfigTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.sourceConfigTable}/"
  def getProcessedHistoryTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.processedHistoryTable}/"
  def getCommitSnapshotTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.commitInfoSnapshotTable}/"
  def getCommitSnapshotTableName(config: OMSConfig): String =
    s"${config.dbName.get}.${config.commitInfoSnapshotTable}"
  def getActionSnapshotTablePath(config: OMSConfig): String =
    s"${getOMSDBPath(config)}/${config.actionSnapshotTable}/"
  def getActionSnapshotTableName(config: OMSConfig): String =
    s"${config.dbName.get}.${config.actionSnapshotTable}"

  val puidCommitDatePartitions = Seq(PUID, COMMIT_DATE)

  private val omsProperties = Map("entity" -> s"$ENTITY_NAME", "oms.version" -> s"$OMS_VERSION")

  def pathConfigTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.pathConfigTable,
      omsConfig.dbName.get,
      pathConfig,
      getPathConfigTablePath(omsConfig),
      Some("Delta OMS Path Config Table"),
      omsProperties
    )
  }

  def sourceConfigDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.sourceConfigTable,
      omsConfig.dbName.get,
      sourceConfig,
      getSourceConfigTablePath(omsConfig),
      Some("Delta OMS Source Config Table"),
      omsProperties
    )
  }

  def rawActionsTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.rawActionTable,
      omsConfig.dbName.get,
      rawAction,
      getRawActionsTablePath(omsConfig),
      Some("Delta OMS Raw Actions Table"),
      omsProperties,
      puidCommitDatePartitions)
  }

  def processedHistoryTableDefinition(omsConfig: OMSConfig): TableDefinition = {
    TableDefinition(omsConfig.processedHistoryTable,
      omsConfig.dbName.get,
      processedHistory,
      getProcessedHistoryTablePath(omsConfig),
      Some("Delta OMS Processed History Table"),
      omsProperties
    )
  }

  def omsDatabaseDefinition(omsConfig: OMSConfig): DatabaseDefinition = {
    DatabaseDefinition(omsConfig.dbName.get,
      Some(getOMSDBPath(omsConfig)),
      Some("Delta OMS Database"),
      omsProperties
    )
  }
}

object Utils extends Utils
