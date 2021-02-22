package com.databricks.labs.deltaods.init

import com.databricks.labs.deltaods.configuration.ODSConfig
import com.databricks.labs.deltaods.utils.ODSUtils._
import com.databricks.labs.deltaods.utils.UtilityOperations._
import org.apache.spark.internal.Logging

import scala.util.{Failure, Success, Try}

trait ODSInitializer extends Serializable with Logging {

  def initializeODSPathConfig(config: ODSConfig, dropAndRecreate: Boolean = false) = {
    if(dropAndRecreate){
      cleanupODS(config)
    }
    createODSDB(config)
    createPathConfigTables(config)
  }

  def createPathConfigTables(config: ODSConfig) = {
    logInfo("Creating the Delta Table Path Config Table on Delta ODS")
    createTableIfAbsent(pathConfigTableDefinition(config))
  }
  def initializeODS(config: ODSConfig, dropAndRecreate: Boolean = false) = {
    if(dropAndRecreate){
      cleanupODS(config)
    }
    createODSDB(config)
    createODSTables(config)
  }

  def createODSDB(config: ODSConfig) = {
    logInfo("Creating the ODS Database on Delta Lake")
    createDatabaseIfAbsent(odsDatabaseDefinition(config))
  }

  def createODSTables(config: ODSConfig) = {
    logInfo("Creating the Latest version table on ODS Delta Lake")
    createTableIfAbsent(lastVersionTableDefinition(config))
    logInfo("Creating the Delta Raw Commit table on ODS Delta Lake")
    createTableIfAbsent(rawCommitTableDefinition(config))
  }

  def cleanupODS(config: ODSConfig) = {
    val deleteDBPath = Try { deleteDirectory(odsDBPath) }
    deleteDBPath match {
      case Success(value) => logInfo(s"Successfully deleted the directory $odsDBPath")
      case Failure(exception) => throw exception
    }
    val dbDrop = Try { dropDatabase(odsConfig.dbName) }
    dbDrop match {
      case Success(value) => logInfo(s"Successfully dropped ODS database ${odsConfig.dbName}")
      case Failure(exception) => throw exception
    }
  }
}
