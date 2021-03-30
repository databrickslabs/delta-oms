package com.databricks.labs.deltaoms.common

import com.databricks.labs.deltaoms.configuration.OMSConfig
import com.databricks.labs.deltaoms.common.OMSUtils._
import com.databricks.labs.deltaoms.utils.UtilityOperations._
import org.apache.spark.internal.Logging

import scala.util.{Failure, Success, Try}

trait OMSInitializer extends Serializable with Logging {

  def initializeOMSPathConfig(config: OMSConfig, dropAndRecreate: Boolean = false) = {
    if(dropAndRecreate){
      cleanupOMS(config)
    }
    createOMSDB(config)
    createPathConfigTables(config)
  }

  def createPathConfigTables(config: OMSConfig) = {
    logInfo("Creating the Delta Table Path Config Table on Delta OMS")
    createTableIfAbsent(pathConfigTableDefinition(config))
  }
  def initializeOMS(config: OMSConfig, dropAndRecreate: Boolean = false) = {
    if(dropAndRecreate){
      cleanupOMS(config)
    }
    createOMSDB(config)
    createOMSTables(config)
  }

  def createOMSDB(config: OMSConfig) = {
    logInfo("Creating the OMS Database on Delta Lake")
    createDatabaseIfAbsent(omsDatabaseDefinition(config))
  }

  def createOMSTables(config: OMSConfig) = {
    logInfo("Creating the Delta Raw Commit table on OMS Delta Lake")
    createTableIfAbsent(rawCommitTableDefinition(config))
    logInfo("Creating the Path Config table on OMS Delta Lake")
    createPathConfigTables(config)
    logInfo("Creating the Delta Raw Actions table on OMS Delta Lake")
    createTableIfAbsent(rawActionsTableDefinition(config))
    logInfo("Creating the Path Snapshot table on OMS Delta Lake")
    createTableIfAbsent(pathSnapshotTableDefinition(config))
  }

  def cleanupOMS(config: OMSConfig) = {
    val deleteDBPath = Try { deleteDirectory(omsDBPath) }
    deleteDBPath match {
      case Success(value) => logInfo(s"Successfully deleted the directory $omsDBPath")
      case Failure(exception) => throw exception
    }
    val dbDrop = Try { dropDatabase(omsConfig.dbName) }
    dbDrop match {
      case Success(value) => logInfo(s"Successfully dropped OMS database ${omsConfig.dbName}")
      case Failure(exception) => throw exception
    }
  }
}
