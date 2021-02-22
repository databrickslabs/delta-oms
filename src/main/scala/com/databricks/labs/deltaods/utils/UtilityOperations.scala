package com.databricks.labs.deltaods.utils

import java.net.URI

import com.databricks.labs.deltaods.model.{DatabaseDefinition, DeltaTableHistory, DeltaTableIdentifierWithLatestVersion, TableDefinition}
import com.databricks.labs.deltaods.utils.UtilityOperations.getDeltaTablesFromMetastore
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.{DeltaErrors, DeltaTableIdentifier, DeltaTableUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.{Failure, Success, Try}

trait UtilityOperations extends Serializable with Logging {

  def fetchMetaStoreDeltaTables(databases: Option[String], pattern: Option[String]) = {
    val odsSrcDatabases = if(databases.isDefined && databases.get.trim.nonEmpty) {
      databases.get.trim.split("[,;:]").map(_.trim).toSeq
    } else {
      Seq.empty[String]
    }
    val tablePattern: String = pattern.map{_.trim}.filterNot{_.isEmpty}.getOrElse("*")
    getDeltaTablesFromMetastore(odsSrcDatabases, tablePattern)
  }

  def getDeltaTablesFromMetastore(databases: Seq[String] = Seq.empty[String],
                                  pattern: String = "*"): Seq[DeltaTableIdentifierWithLatestVersion] = {
    val spark = SparkSession.active
    val sessionCatalog = spark.sessionState.catalog
    val databaseList = if(databases.nonEmpty)
      sessionCatalog.listDatabases.filter(databases.contains(_))
    else
      sessionCatalog.listDatabases
    val allTables = databaseList.flatMap(dbName =>
      sessionCatalog.listTables(dbName,pattern,includeLocalTempViews = false))
    allTables.flatMap(DeltaTableIdentifierWithLatestVersion(spark,_))
  }

  def getDeltaPathFromDelTableIdentifiers(deltaTableIds: Seq[DeltaTableIdentifier]): Seq[Path] = {
    val spark = SparkSession.active
    deltaTableIds.map(_.getPath(spark))
  }

  def createDatabaseIfAbsent(dbDefn: DatabaseDefinition): Unit = {
    val spark = SparkSession.active
    val dBCreateSQL = new StringBuilder(s"CREATE DATABASE IF NOT EXISTS ${dbDefn.databaseName} ")
    if (dbDefn.comment.nonEmpty) {
      dBCreateSQL.append(s"COMMENT '${dbDefn.comment.get}' ")
    }
    if (dbDefn.location.nonEmpty) {
      dBCreateSQL.append(s"LOCATION '${dbDefn.location.get}' ")
    }
    if (dbDefn.properties.nonEmpty) {
      val tableProperties = dbDefn.properties.map(_.productIterator.mkString("'", "'='", "'")).mkString(",")
      dBCreateSQL.append(s"WITH DBPROPERTIES($tableProperties) ")
    }
    logDebug(s"CREATING DATABASE using SQL => ${dBCreateSQL.toString()}")
    Try {
      spark.sql(dBCreateSQL.toString())
    } match {
      case Success(value) => logInfo(s"Successfully created the database ${dbDefn.databaseName}")
      case Failure(exception) => throw new RuntimeException(s"Unable to create the Database: $exception")
    }
  }

  def createTableIfAbsent(tableDefn: TableDefinition): Unit = {
    val spark = SparkSession.active
    val fqTableName = s"${tableDefn.databaseName}.${tableDefn.tableName}"
    val tableId = spark.sessionState.sqlParser.parseTableIdentifier(s"${fqTableName}")
    if (!DeltaTable.isDeltaTable(tableDefn.path) && !DeltaTableUtils.isDeltaTable(spark, tableId)) {
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tableDefn.schema)
      val dataFrameCreateWriter = emptyDF
        .writeTo(fqTableName)
        .tableProperty("location",tableDefn.path)
      val dataFrameCreateWriterWithComment = tableDefn.comment.foldLeft(dataFrameCreateWriter){
        (dfw, c) => dfw.tableProperty("comment",c)
      }
      val dataFrameCreateWriterWithProperties = tableDefn.properties.toList
        .foldLeft(dataFrameCreateWriterWithComment){
          (dfw, kv) => dfw.tableProperty(kv._1,kv._2)
      }
      if (tableDefn.partitionColumnNames.nonEmpty) {
        val partitionColumns = tableDefn.partitionColumnNames.map(col)
        dataFrameCreateWriterWithProperties
          .using("delta")
          .partitionedBy(partitionColumns.head, partitionColumns.tail: _*)
          .createOrReplace()
      } else {
        dataFrameCreateWriterWithProperties
          .using("delta")
          .createOrReplace()
      }
    }
  }

  def getHistoryFromTableVersion(tableDefn: DeltaTableIdentifierWithLatestVersion,
                                 versionFetchSize: Option[Long] = None,
                                 fetchEarliestAvailable: Boolean = true): DeltaTableHistory = {
    val spark = SparkSession.active
    val deltaLog = tableDefn.table.getDeltaLog(spark)
    val earliestVersionOpt = deltaLog.store.listFrom(FileNames.deltaFile(deltaLog.logPath, 0))
      .filter(f => FileNames.isDeltaFile(f.getPath))
      .take(1).toArray.headOption
    if (earliestVersionOpt.isEmpty) {
      logInfo(s"No Delta commits found for $tableDefn")
      DeltaTableHistory(tableDefn,
        Seq.empty[CommitInfo])
    } else {
      val startingVersion = math.max(FileNames.deltaVersion(earliestVersionOpt.get.getPath),tableDefn.version)
      val endVersion: Option[Long] = versionFetchSize match {
        case Some(fs) => Some(startingVersion+fs)
        case _ => None
      }
      DeltaTableHistory(tableDefn,
        tableDefn.table.getDeltaLog(spark).history.getHistory(startingVersion, endVersion))
    }
  }

  def deleteDirectory(dirName: String) = {
    val fileSystem = FileSystem.get(new URI(dirName),
      SparkSession.active.sparkContext.hadoopConfiguration)
    fileSystem.delete(new Path(dirName), true)
  }

  def dropDatabase(dbName: String) = {
    SparkSession.active.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
  }
}

object UtilityOperations extends UtilityOperations
