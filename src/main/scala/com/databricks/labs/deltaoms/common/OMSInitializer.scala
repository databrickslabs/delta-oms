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

package com.databricks.labs.deltaoms.common

import scala.util.{Failure, Success, Try}
import com.databricks.labs.deltaoms.configuration.OMSConfig
import OMSUtils._
import com.databricks.labs.deltaoms.utils.UtilityOperations._

import org.apache.spark.internal.Logging

trait OMSInitializer extends Serializable with Logging {

  def initializeOMSPathConfig(config: OMSConfig, dropAndRecreate: Boolean = false): Unit = {
    if (dropAndRecreate) {
      cleanupOMS(config)
    }
    createOMSDB(config)
    createPathConfigTables(config)
  }

  def initializeOMS(config: OMSConfig, dropAndRecreate: Boolean = false): Unit = {
    if (dropAndRecreate) {
      cleanupOMS(config)
    }
    createOMSDB(config)
    createOMSTables(config)
    /* Uncomment to add the new created OMS Database to be monitored by OMS
    if (dropAndRecreate) {
      populateOMSSourceConfigTableWithSelf(config.dbName, config.sourceConfigTable)
    } */
  }

  def createOMSDB(config: OMSConfig): Unit = {
    logInfo("Creating the OMS Database on Delta Lake")
    createDatabaseIfAbsent(omsDatabaseDefinition(config))
  }

  def createOMSTables(config: OMSConfig): Unit = {
    logInfo("Creating the EXTERNAL Source Config table on OMS Delta Lake")
    createTableIfAbsent(sourceConfigDefinition(config))
    logInfo("Creating the INTERNAL Path Config table on OMS Delta Lake")
    createPathConfigTables(config)
    logInfo("Creating the Delta Raw Actions table on OMS Delta Lake")
    createTableIfAbsent(rawActionsTableDefinition(config))
    logInfo("Creating the Processing History table on OMS Delta Lake")
    createTableIfAbsent(processedHistoryTableDefinition(config))
  }

  def createPathConfigTables(config: OMSConfig): Unit = {
    logInfo("Creating the Delta Table Path Config Table on Delta OMS")
    createTableIfAbsent(pathConfigTableDefinition(config))
  }

  def cleanupOMS(config: OMSConfig): Unit = {
    val deleteDBPath = Try {
      deleteDirectory(getOMSDBPath(config))
    }
    deleteDBPath match {
      case Success(value) => logInfo(s"Successfully deleted the directory ${getOMSDBPath(config)}")
      case Failure(exception) => throw exception
    }
    val dbDrop = Try {
      dropDatabase(config.dbName.get)
    }
    dbDrop match {
      case Success(value) => logInfo(s"Successfully dropped OMS database ${config.dbName}")
      case Failure(exception) => throw exception
    }
  }
}
